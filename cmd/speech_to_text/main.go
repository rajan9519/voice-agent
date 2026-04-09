package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"voiceagent/internal/stt"
)

type speechToTextApp struct {
	pipeline *stt.SpeechToText
}

func newSpeechToTextApp(pipeline *stt.SpeechToText) *speechToTextApp {
	return &speechToTextApp{pipeline: pipeline}
}

func (a *speechToTextApp) run(ctx context.Context) (err error) {
	if err = a.pipeline.Start(ctx); err != nil {
		return err
	}

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if stopErr := a.pipeline.Stop(stopCtx); stopErr != nil && !errors.Is(stopErr, context.Canceled) && err == nil {
			err = stopErr
		}
	}()

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Real-Time Speech-to-Text")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Start speaking! Press Ctrl+C to stop.")

	transcripts := a.pipeline.Results()
	errorCh := a.pipeline.Errors()
	var finalBuilder strings.Builder

	for transcripts != nil || errorCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-transcripts:
			if !ok {
				transcripts = nil
				continue
			}
			if event.Err != nil {
				fmt.Fprintf(os.Stderr, "\nprovider error: %v\n", event.Err)
				continue
			}
			switch event.Type {
			case "transcript":
				if event.IsFinal {
					if finalBuilder.Len() > 0 {
						finalBuilder.WriteByte(' ')
					}
					finalBuilder.WriteString(event.Text)
					fmt.Printf("\r%s\n", event.Text)
				} else if event.Text != "" {
					fmt.Printf("\r%s...", event.Text)
				}
			case "flush_done":
				fmt.Println("\nFlush complete")
			case "done":
				transcripts = nil
			default:
				if event.Text != "" {
					fmt.Printf("\r%s\n", event.Text)
				}
			}
		case err, ok := <-errorCh:
			if !ok {
				errorCh = nil
				continue
			}
			if err != nil {
				return err
			}
		}
	}

	if finalBuilder.Len() > 0 {
		fmt.Println("\n" + strings.Repeat("=", 60))
		fmt.Println("Complete Transcript:")
		fmt.Println(strings.Repeat("=", 60))
		fmt.Println(finalBuilder.String())
		fmt.Println(strings.Repeat("=", 60))
	}

	return nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		// Ignore missing .env file; environment variables may already be present.
	}

	languageFlag := flag.String("language", "english", "Language for transcription")
	sarvamModel := flag.String("sarvam-model", "saarika:v2.5", "Sarvam AI model version")
	enableHighVAD := flag.Bool("enable-high-vad", false, "Enable high VAD sensitivity for Sarvam")
	enableVADSignals := flag.Bool("enable-vad-signals", true, "Enable VAD signals from Sarvam")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := stt.ProviderConfig{
		CartesiaKey: os.Getenv("CARTESIA_API_KEY"),
		SarvamKey:   os.Getenv("SARVAM_API_KEY"),
		SarvamModel: *sarvamModel,
		HighVAD:     *enableHighVAD,
		VADSignals:  *enableVADSignals,
	}

	provider, languageCode, err := stt.SelectProvider(*languageFlag, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "provider selection error: %v\n", err)
		os.Exit(1)
	}

	recorder, err := stt.NewPortAudioRecorder()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize microphone: %v\n", err)
		os.Exit(1)
	}

	pipeline, err := stt.New(provider, recorder, stt.Options{Language: languageCode})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to configure pipeline: %v\n", err)
		os.Exit(1)
	}

	app := newSpeechToTextApp(pipeline)
	if err := app.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "speech-to-text error: %v\n", err)
		os.Exit(1)
	}
}
