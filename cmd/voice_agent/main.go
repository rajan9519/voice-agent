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
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"voiceagent/internal/agent"
	"voiceagent/internal/llm"
	"voiceagent/internal/stt"
	"voiceagent/internal/tts"
)

// voiceAgent is the common interface implemented by both agent types.
type voiceAgent interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

func main() {
	_ = godotenv.Load()

	var (
		mode                  = flag.String("mode", "conversation", "Agent mode: conversation or translation")
		sourceLanguage        = flag.String("source-language", "english", "Language spoken into the microphone")
		targetLanguage        = flag.String("target-language", "english", "Language to translate into (translation mode only)")
		sttProvider           = flag.String("stt-provider", "", "STT provider (cartesia|sarvam) - auto-detects if not specified")
		ttsProvider           = flag.String("tts-provider", "cartesia", "TTS provider (cartesia|sarvam)")
		ttsVoice              = flag.String("tts-voice", "default", "Voice preset or provider voice ID")
		ttsLanguage           = flag.String("tts-language", "english", "TTS language preset (Sarvam only)")
		ttsModel              = flag.String("tts-model", "", "Override the TTS model identifier")
		ttsSampleRate         = flag.Int("tts-sample-rate", 22050, "TTS output sample rate")
		sttSarvamModel        = flag.String("stt-sarvam-model", "saarika:v2.5", "Sarvam STT model version")
		sttEnableHighVAD      = flag.Bool("stt-enable-high-vad", false, "Enable high VAD sensitivity for Sarvam STT")
		sttEnableVADSignals   = flag.Bool("stt-enable-vad-signals", true, "Enable Sarvam VAD speech markers")
		usePartialTranscripts = flag.Bool("use-partials", false, "Forward partial STT transcripts to the agent")
		flushThreshold        = flag.Int("flush-threshold", 120, "Minimum characters before forcing a TTS flush")
		llmModel              = flag.String("model", "", "Override the LLM model identifier")
	)
	flag.Parse()

	*mode = strings.TrimSpace(strings.ToLower(*mode))
	if *mode != "conversation" && *mode != "translation" {
		log.Fatalf("invalid --mode %q: must be conversation or translation", *mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sttCfg := stt.ProviderConfig{
		CartesiaKey: os.Getenv("CARTESIA_API_KEY"),
		SarvamKey:   os.Getenv("SARVAM_API_KEY"),
		SarvamModel: strings.TrimSpace(*sttSarvamModel),
		HighVAD:     *sttEnableHighVAD,
		VADSignals:  *sttEnableVADSignals,
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

	llmClient, err := llm.New(llm.Config{
		DefaultModel: strings.TrimSpace(*llmModel),
	})
	if err != nil {
		_ = player.Close()
		_ = recorder.Close()
		log.Fatalf("llm client: %v", err)
	}

	var va voiceAgent
	var agentLabel string

	switch *mode {
	case "translation":
		va, err = agent.NewTranslationAgent(sttPipeline, ttsEngine, llmClient, player, agent.TranslationOptions{
			TargetLanguage:        strings.TrimSpace(*targetLanguage),
			TranslationModel:      strings.TrimSpace(*llmModel),
			FlushThreshold:        *flushThreshold,
			UsePartialTranscripts: *usePartialTranscripts,
		})
		agentLabel = "Real-Time Translation Agent"
	default: // "conversation"
		va, err = agent.NewConversationAgent(sttPipeline, ttsEngine, llmClient, player, agent.ConversationOptions{
			ConversationModel:       strings.TrimSpace(*llmModel),
			FlushThreshold:          *flushThreshold,
			TranscriptQueueCapacity: 5,
			UsePartialTranscripts:   *usePartialTranscripts,
		})
		agentLabel = "Real-Time Conversation Agent"
	}
	if err != nil {
		_ = player.Close()
		_ = recorder.Close()
		log.Fatalf("configure agent: %v", err)
	}

	if err := va.Start(ctx); err != nil {
		_ = va.Stop(context.Background())
		_ = recorder.Close()
		log.Fatalf("start agent: %v", err)
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("🎧  %s\n", agentLabel)
	fmt.Println(strings.Repeat("=", 60))
	titleCaser := cases.Title(language.English)
	fmt.Printf("Listening in %s", titleCaser.String(strings.TrimSpace(*sourceLanguage)))
	if *mode == "translation" {
		fmt.Printf(" → speaking in %s", titleCaser.String(strings.TrimSpace(*targetLanguage)))
	}
	fmt.Println(".")
	fmt.Println("Press Ctrl+C to stop.")

	<-ctx.Done()

	fmt.Println("\nStopping...")
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := va.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
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
		voiceID := tts.ResolveSarvamVoice(voice, model)
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
