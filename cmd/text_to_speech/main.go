package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"voiceagent/internal/tts"
)

func main() {
	_ = godotenv.Load()

	var (
		providerFlag    = flag.String("provider", "cartesia", "tts provider (cartesia|sarvam)")
		voiceFlag       = flag.String("voice", "default", "voice preset or provider voice id")
		languageFlag    = flag.String("language", "english", "language preset (sarvam)")
		modelFlag       = flag.String("model", "", "override model id")
		sampleRateFlag  = flag.Int("sample-rate", 22050, "output sample rate")
		interactiveFlag = flag.Bool("interactive", false, "stream text interactively from stdin")
		textFlag        = flag.String("text", "", "text to synthesize (non-interactive)")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	providerName := strings.ToLower(strings.TrimSpace(*providerFlag))
	sampleRate := *sampleRateFlag

	engine, shutdown, err := buildEngine(ctx, providerName, sampleRate, *voiceFlag, *languageFlag, *modelFlag)
	if err != nil {
		log.Fatalf("configure: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			log.Printf("cleanup error: %v\n", err)
		}
	}()

	if *interactiveFlag {
		fmt.Println("\n🎙️  Interactive TTS streaming. Type text and press Enter (Ctrl+D to exit).")
		scanner := bufio.NewScanner(os.Stdin)
		if err := engine.DrainScanner(ctx, scanner); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			log.Printf("interactive stop: %v\n", err)
		}
		return
	}

	text := strings.TrimSpace(*textFlag)
	if text == "" {
		text = strings.TrimSpace(strings.Join(flag.Args(), " "))
	}
	if text == "" {
		log.Fatal("no text provided; pass --text or use --interactive")
	}

	fmt.Printf("🔊 Synthesizing with %s (%s voice)...\n", cases.Title(language.English).String(providerName), *voiceFlag)
	if err := engine.Speak(ctx, text); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("synthesis failed: %v", err)
	}
	fmt.Println("✅ Done.")
}

func buildEngine(
	ctx context.Context,
	providerName string,
	sampleRate int,
	voice string,
	language string,
	model string,
) (*tts.TextToSpeech, func(context.Context) error, error) {
	switch providerName {
	case "cartesia":
		apiKey := strings.TrimSpace(os.Getenv("CARTESIA_API_KEY"))
		if apiKey == "" {
			return nil, nil, fmt.Errorf("CARTESIA_API_KEY not set")
		}
		voiceID := tts.ResolveCartesiaVoice(voice)
		provider := tts.NewCartesiaProvider(apiKey, sampleRate, model)
		engine, shutdown, err := startEngine(ctx, provider, tts.Options{
			SampleRate: sampleRate,
			VoiceID:    voiceID,
			ModelID:    model,
		}, sampleRate)
		return engine, shutdown, err
	case "sarvam":
		apiKey := strings.TrimSpace(os.Getenv("SARVAM_API_KEY"))
		if apiKey == "" {
			return nil, nil, fmt.Errorf("SARVAM_API_KEY not set")
		}
		languageCode := tts.ResolveSarvamLanguage(language)
		voiceID := tts.ResolveSarvamVoice(voice, model)
		provider := tts.NewSarvamProvider(apiKey, sampleRate, languageCode, model)
		engine, shutdown, err := startEngine(ctx, provider, tts.Options{
			SampleRate: sampleRate,
			VoiceID:    voiceID,
			ModelID:    model,
			Language:   languageCode,
		}, sampleRate)
		return engine, shutdown, err
	default:
		return nil, nil, fmt.Errorf("unsupported provider %q", providerName)
	}
}

func startEngine(
	ctx context.Context,
	provider tts.Provider,
	opts tts.Options,
	sampleRate int,
) (*tts.TextToSpeech, func(context.Context) error, error) {
	engine, err := tts.New(provider, opts)
	if err != nil {
		return nil, nil, err
	}
	if err := engine.Start(ctx); err != nil {
		return nil, nil, err
	}

	shutdown, err := attachSpeaker(ctx, engine, sampleRate)
	if err != nil {
		_ = engine.Stop(context.Background())
		return nil, nil, err
	}
	return engine, shutdown, nil
}

func attachSpeaker(ctx context.Context, engine *tts.TextToSpeech, sampleRate int) (func(context.Context) error, error) {
	player, err := tts.NewPortAudioPlayer(sampleRate)
	if err != nil {
		return nil, fmt.Errorf("audio output: %w", err)
	}

	playbackCtx, playbackCancel := context.WithCancel(ctx)
	var playbackWG sync.WaitGroup
	playbackWG.Add(1)

	errCh := make(chan error, 1)

	go func() {
		defer playbackWG.Done()
		for {
			select {
			case <-playbackCtx.Done():
				return
			case chunk, ok := <-engine.AudioStream():
				if !ok {
					return
				}
				if err := player.Play(chunk); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	return func(stopCtx context.Context) error {
		var errs []error

		playbackCancel()
		playbackWG.Wait()

		if err := engine.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			errs = append(errs, err)
		}

		if err := player.Close(); err != nil {
			errs = append(errs, err)
		}

		select {
		case err := <-errCh:
			if err != nil {
				errs = append(errs, fmt.Errorf("audio playback: %w", err))
			}
		default:
		}

		if len(errs) == 0 {
			return nil
		}
		return errors.Join(errs...)
	}, nil
}
