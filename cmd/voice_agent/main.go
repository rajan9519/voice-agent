package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"voiceagent/internal/agent"
	"voiceagent/internal/llm"
	"voiceagent/internal/stt"
	"voiceagent/internal/tts"
)

func main() {
	_ = godotenv.Load()

	var (
		sourceLanguage        = flag.String("source-language", "english", "Language spoken into the microphone")
		targetLanguage        = flag.String("target-language", "english", "Language to translate into")
		sttProvider           = flag.String("stt-provider", "", "STT provider (cartesia|sarvam) - auto-detects if not specified")
		ttsProvider           = flag.String("tts-provider", "cartesia", "TTS provider (cartesia|sarvam)")
		ttsVoice              = flag.String("tts-voice", "default", "Voice preset or provider voice ID")
		ttsLanguage           = flag.String("tts-language", "english", "TTS language preset (Sarvam only)")
		ttsModel              = flag.String("tts-model", "", "Override the TTS model identifier")
		ttsSampleRate         = flag.Int("tts-sample-rate", 22050, "TTS output sample rate")
		sttSarvamModel        = flag.String("stt-sarvam-model", "saarika:v2.5", "Sarvam STT model version")
		sttDisableHighVAD     = flag.Bool("stt-disable-high-vad", false, "Disable high VAD sensitivity for Sarvam STT")
		sttDisableVADSignals  = flag.Bool("stt-disable-vad-signals", false, "Disable Sarvam VAD speech markers")
		usePartialTranscripts = flag.Bool("use-partials", false, "Forward partial STT transcripts to the translator")
		flushThreshold        = flag.Int("flush-threshold", 120, "Minimum characters before forcing a TTS flush")
		translateModel        = flag.String("translate-model", "", "Override the translation model identifier")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sttCfg := stt.ProviderConfig{
		CartesiaKey: os.Getenv("CARTESIA_API_KEY"),
		SarvamKey:   os.Getenv("SARVAM_API_KEY"),
		SarvamModel: strings.TrimSpace(*sttSarvamModel),
		HighVAD:     !*sttDisableHighVAD,
		VADSignals:  !*sttDisableVADSignals,
	}

	var sttProviderInst stt.Provider
	var languageCode string
	var err error

	if *sttProvider != "" {
		// User explicitly specified a provider
		sttProviderInst, languageCode, err = stt.SelectProviderExplicit(strings.TrimSpace(*sttProvider), *sourceLanguage, sttCfg)
	} else {
		// Auto-detect provider based on language
		sttProviderInst, languageCode, err = stt.SelectProvider(*sourceLanguage, sttCfg)
	}
	if err != nil {
		log.Fatalf("speech provider: %v", err)
	}

	recorder, err := stt.NewPortAudioRecorder()
	if err != nil {
		log.Fatalf("microphone: %v", err)
	}

	sttPipeline, err := stt.New(sttProviderInst, recorder, stt.Options{Language: languageCode})
	if err != nil {
		_ = recorder.Close()
		log.Fatalf("stt pipeline: %v", err)
	}

	ttsEngine, err := configureTTSEngine(*ttsProvider, *ttsVoice, *ttsLanguage, *ttsModel, *ttsSampleRate)
	if err != nil {
		_ = recorder.Close()
		log.Fatalf("tts engine: %v", err)
	}

	player, err := tts.NewPortAudioPlayer(*ttsSampleRate)
	if err != nil {
		_ = recorder.Close()
		log.Fatalf("audio output: %v", err)
	}

	translator, err := llm.New(llm.Config{
		DefaultModel: strings.TrimSpace(*translateModel),
	})
	if err != nil {
		_ = player.Close()
		_ = recorder.Close()
		log.Fatalf("translator client: %v", err)
	}

	agentOpts := agent.Options{
		TargetLanguage:        strings.TrimSpace(*targetLanguage),
		TranslationModel:      strings.TrimSpace(*translateModel),
		FlushThreshold:        *flushThreshold,
		UsePartialTranscripts: *usePartialTranscripts,
	}

	voiceAgent, err := agent.NewTranslationAgent(sttPipeline, ttsEngine, translator, player, agentOpts)
	if err != nil {
		_ = player.Close()
		_ = recorder.Close()
		log.Fatalf("configure agent: %v", err)
	}

	if err := voiceAgent.Start(ctx); err != nil {
		_ = voiceAgent.Stop(context.Background())
		_ = recorder.Close()
		log.Fatalf("start agent: %v", err)
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("🎧  Real-Time Translation Agent")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Listening in %s → speaking in %s.\n", strings.Title(strings.TrimSpace(*sourceLanguage)), strings.Title(agentOpts.TargetLanguage))
	fmt.Println("Press Ctrl+C to stop.\n")

	<-ctx.Done()

	fmt.Println("\nStopping...")
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := voiceAgent.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("agent shutdown error: %v\n", err)
		os.Exit(1)
	}
}

func configureTTSEngine(providerName, voice, language, model string, sampleRate int) (*tts.TextToSpeech, error) {
	name := strings.ToLower(strings.TrimSpace(providerName))

	switch name {
	case "cartesia":
		apiKey := strings.TrimSpace(os.Getenv("CARTESIA_API_KEY"))
		if apiKey == "" {
			return nil, errors.New("tts: CARTESIA_API_KEY not set")
		}
		voiceID := tts.ResolveCartesiaVoice(voice)
		provider := tts.NewCartesiaProvider(apiKey, sampleRate, model)
		return tts.New(provider, tts.Options{
			SampleRate: sampleRate,
			VoiceID:    voiceID,
			ModelID:    model,
		})
	case "sarvam":
		apiKey := strings.TrimSpace(os.Getenv("SARVAM_API_KEY"))
		if apiKey == "" {
			return nil, errors.New("tts: SARVAM_API_KEY not set")
		}
		voiceID := tts.ResolveSarvamVoice(voice)
		languageCode := tts.ResolveSarvamLanguage(language)
		provider := tts.NewSarvamProvider(apiKey, sampleRate, languageCode, model)
		return tts.New(provider, tts.Options{
			SampleRate: sampleRate,
			VoiceID:    voiceID,
			ModelID:    model,
			Language:   languageCode,
		})
	default:
		return nil, fmt.Errorf("tts: unsupported provider %q", providerName)
	}
}
