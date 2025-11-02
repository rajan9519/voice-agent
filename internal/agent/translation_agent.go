package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"voiceagent/internal/llm"
	"voiceagent/internal/stt"
	"voiceagent/internal/tts"
)

const (
	defaultFlushThreshold   = 120
	defaultTranscriptBuffer = 8
)

// Translator captures the subset of the LLM client used by the agent.
type Translator interface {
	StreamTranslate(ctx context.Context, text string, opts llm.TranslateOptions) (*llm.StreamResponse, error)
}

// AudioPlayer consumes PCM chunks from the TTS engine.
type AudioPlayer interface {
	Play(tts.AudioChunk) error
	Close() error
}

// Options configures the behaviour of the translation agent.
type Options struct {
	TargetLanguage          string
	TranslationModel        string
	FlushThreshold          int
	TranscriptQueueCapacity int
	UsePartialTranscripts   bool
}

// TranslationAgent wires STT, LLM translation, and TTS playback into a single pipeline.
type TranslationAgent struct {
	stt        *stt.SpeechToText
	tts        *tts.TextToSpeech
	translator Translator
	player     AudioPlayer
	opts       Options

	ctx    context.Context
	cancel context.CancelFunc

	transcripts chan string
	wg          sync.WaitGroup

	errOnce sync.Once
	err     error
}

// NewTranslationAgent validates dependencies and returns a ready-to-start agent.
func NewTranslationAgent(
	sttPipeline *stt.SpeechToText,
	ttsEngine *tts.TextToSpeech,
	translator Translator,
	player AudioPlayer,
	opts Options,
) (*TranslationAgent, error) {
	if sttPipeline == nil {
		return nil, errors.New("agent: speech-to-text pipeline is required")
	}
	if ttsEngine == nil {
		return nil, errors.New("agent: text-to-speech engine is required")
	}
	if translator == nil {
		return nil, errors.New("agent: translator client is required")
	}
	if player == nil {
		return nil, errors.New("agent: audio player is required")
	}

	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = defaultFlushThreshold
	}
	if opts.TranscriptQueueCapacity <= 0 {
		opts.TranscriptQueueCapacity = defaultTranscriptBuffer
	}

	return &TranslationAgent{
		stt:         sttPipeline,
		tts:         ttsEngine,
		translator:  translator,
		player:      player,
		opts:        opts,
		transcripts: make(chan string, opts.TranscriptQueueCapacity),
	}, nil
}

// Start launches background processing and begins streaming audio and transcripts.
func (a *TranslationAgent) Start(parent context.Context) error {
	if a.ctx != nil {
		return errors.New("agent: already started")
	}
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	a.ctx = ctx
	a.cancel = cancel
	a.transcripts = make(chan string, a.opts.TranscriptQueueCapacity)

	if err := a.tts.Start(ctx); err != nil {
		a.reset()
		return fmt.Errorf("agent: start tts: %w", err)
	}

	if err := a.stt.Start(ctx); err != nil {
		_ = a.tts.Stop(context.Background())
		a.reset()
		return fmt.Errorf("agent: start stt: %w", err)
	}

	a.wg.Add(3)
	go a.playbackLoop()
	go a.transcriptLoop()
	go a.translationLoop()

	return nil
}

// Stop cancels background work and releases resources.
func (a *TranslationAgent) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if a.ctx == nil {
		if a.player != nil {
			if err := a.player.Close(); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("agent: close player: %w", err)
			}
		}
		return nil
	}

	a.cancel()

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	var errs []error

	if err := a.stt.Stop(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		errs = append(errs, fmt.Errorf("agent: stop stt: %w", err))
	}
	if err := a.tts.Stop(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		errs = append(errs, fmt.Errorf("agent: stop tts: %w", err))
	}
	if err := a.player.Close(); err != nil && !errors.Is(err, context.Canceled) {
		errs = append(errs, fmt.Errorf("agent: close player: %w", err))
	}

	a.reset()

	if len(errs) > 0 {
		errs = append(errs, a.err)
		return errors.Join(errs...)
	}

	return a.err
}

// Err returns the first asynchronous error encountered by the agent, if any.
func (a *TranslationAgent) Err() error {
	return a.err
}

func (a *TranslationAgent) reset() {
	a.ctx = nil
	a.cancel = nil
	a.transcripts = nil
}

func (a *TranslationAgent) playbackLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.ctx.Done():
			return
		case chunk, ok := <-a.tts.AudioStream():
			if !ok {
				return
			}
			if err := a.player.Play(chunk); err != nil {
				a.fail(fmt.Errorf("playback: %w", err))
				return
			}
		}
	}
}

func (a *TranslationAgent) transcriptLoop() {
	defer a.wg.Done()
	defer close(a.transcripts)

	results := a.stt.Results()
	errorsCh := a.stt.Errors()

	for results != nil || errorsCh != nil {
		select {
		case <-a.ctx.Done():
			return
		case event, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			if event.Err != nil {
				a.fail(fmt.Errorf("stt provider: %w", event.Err))
				continue
			}

			if !event.IsFinal && !a.opts.UsePartialTranscripts {
				continue
			}

			text := strings.TrimSpace(event.Text)
			if text == "" {
				continue
			}

			select {
			case a.transcripts <- text:
			case <-a.ctx.Done():
				return
			}
		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if err != nil {
				a.fail(fmt.Errorf("recorder: %w", err))
			}
		}
	}
}

func (a *TranslationAgent) translationLoop() {
	defer a.wg.Done()

	for {
		select {
		case <-a.ctx.Done():
			return
		case text, ok := <-a.transcripts:
			if !ok {
				return
			}
			if err := a.translateAndSpeak(text); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				a.fail(err)
			}
		}
	}
}

func (a *TranslationAgent) translateAndSpeak(text string) error {
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	stream, err := a.translator.StreamTranslate(ctx, text, llm.TranslateOptions{
		TargetLanguage: a.opts.TargetLanguage,
		Model:          a.opts.TranslationModel,
	})
	if err != nil {
		return fmt.Errorf("translate: %w", err)
	}

	var buffer strings.Builder

	for stream.Text != nil || stream.Err != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delta, ok := <-stream.Text:
			if !ok {
				stream.Text = nil
				continue
			}
			buffer.WriteString(delta)
			if err := a.flushBuffer(&buffer, false); err != nil {
				return err
			}
		case streamErr, ok := <-stream.Err:
			if !ok {
				stream.Err = nil
				continue
			}
			if streamErr != nil {
				return fmt.Errorf("translate stream: %w", streamErr)
			}
		}
	}

	return a.flushBuffer(&buffer, true)
}

func (a *TranslationAgent) flushBuffer(buf *strings.Builder, force bool) error {
	raw := buf.String()
	if raw == "" {
		return nil
	}

	var remainder strings.Builder
	start := 0

	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '.', '!', '?', '\n':
			end := i + 1
			segment := strings.TrimSpace(raw[start:end])
			if segment != "" {
				if err := a.tts.Enqueue(segment); err != nil {
					return fmt.Errorf("tts enqueue: %w", err)
				}
			}
			start = end
		}
	}

	remainder.WriteString(strings.TrimLeft(raw[start:], " \n\t"))

	if remainder.Len() >= a.opts.FlushThreshold || (force && remainder.Len() > 0) {
		segment := strings.TrimSpace(remainder.String())
		if segment != "" {
			if err := a.tts.Enqueue(segment); err != nil {
				return fmt.Errorf("tts enqueue: %w", err)
			}
		}
		remainder.Reset()
	}

	buf.Reset()
	buf.WriteString(remainder.String())
	return nil
}

func (a *TranslationAgent) fail(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	a.errOnce.Do(func() {
		a.err = err
		if a.cancel != nil {
			a.cancel()
		}
	})
}
