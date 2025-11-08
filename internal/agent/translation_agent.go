package agent

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	transcripts chan transcriptPayload
	wg          sync.WaitGroup

	errOnce sync.Once
	err     error

	sequenceCounter int64
}

type transcriptPayload struct {
	id                 int64
	text               string
	partial            bool
	transcribeDuration time.Duration
	start              time.Time
	capturedAt         time.Time
}

type translationStats struct {
	Duration        time.Duration
	Characters      int
	Segments        int
	EnqueueDuration time.Duration
	outputBuilder   strings.Builder
}

func (s *translationStats) appendOutput(segment string) {
	if segment == "" {
		return
	}
	s.Characters += len(segment)
	if s.outputBuilder.Len() > 0 {
		s.outputBuilder.WriteByte(' ')
	}
	s.outputBuilder.WriteString(segment)
}

func (s *translationStats) OutputText() string {
	return strings.TrimSpace(s.outputBuilder.String())
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
		transcripts: make(chan transcriptPayload, opts.TranscriptQueueCapacity),
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
	a.transcripts = make(chan transcriptPayload, a.opts.TranscriptQueueCapacity)

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

	var (
		utteranceStart time.Time
		currentSeq     int64 = -1
	)

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
				a.debugf("STT provider error: %v", event.Err)
				a.fail(fmt.Errorf("stt provider: %w", event.Err))
				continue
			}

			now := time.Now()

			if event.Type == "transcript" {
				if !event.IsFinal {
					if utteranceStart.IsZero() {
						utteranceStart = now
					}
					if currentSeq == -1 {
						currentSeq = a.nextSequence()
					}

					text := strings.TrimSpace(event.Text)
					if !a.opts.UsePartialTranscripts {
						if text != "" {
							a.debugf("STT partial ignored (final-only mode): %q", truncate(text, 120))
						}
						continue
					}
					if text == "" {
						continue
					}

					elapsed := now.Sub(utteranceStart)
					a.debugf(
						"STT partial #%d provider=%s elapsed=%.2fs text=%q",
						currentSeq,
						event.Provider,
						elapsed.Seconds(),
						truncate(text, 160),
					)

					payload := transcriptPayload{
						id:                 currentSeq,
						text:               text,
						partial:            true,
						transcribeDuration: elapsed,
						start:              utteranceStart,
						capturedAt:         now,
					}

					select {
					case a.transcripts <- payload:
					case <-a.ctx.Done():
						return
					}
					continue
				}

				text := strings.TrimSpace(event.Text)
				if text == "" {
					continue
				}

				if utteranceStart.IsZero() {
					utteranceStart = now
				}
				if currentSeq == -1 {
					currentSeq = a.nextSequence()
				}

				duration := now.Sub(utteranceStart)
				seq := currentSeq
				if seq == -1 {
					seq = a.nextSequence()
				}

				a.debugf(
					"STT final #%d provider=%s duration=%.2fs text=%q",
					seq,
					event.Provider,
					duration.Seconds(),
					truncate(text, 240),
				)

				payload := transcriptPayload{
					id:                 seq,
					text:               text,
					partial:            false,
					transcribeDuration: duration,
					start:              utteranceStart,
					capturedAt:         now,
				}

				select {
				case a.transcripts <- payload:
				case <-a.ctx.Done():
					return
				}

				utteranceStart = time.Time{}
				currentSeq = -1
				continue
			}

			if isSpeechStartEvent(event.Type) {
				if utteranceStart.IsZero() {
					utteranceStart = now
				}
				if currentSeq == -1 {
					currentSeq = a.nextSequence()
				}
			}

			text := strings.TrimSpace(event.Text)
			if text == "" {
				a.debugf("STT event %q", event.Type)
				continue
			}

			a.debugf("STT event %q: %s", event.Type, truncate(text, 120))
		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if err != nil {
				a.debugf("Recorder error: %v", err)
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
		case payload, ok := <-a.transcripts:
			if !ok {
				return
			}

			a.debugf(
				"Translation start #%d partial=%t text=%q",
				payload.id,
				payload.partial,
				truncate(payload.text, 200),
			)

			stats, err := a.translateAndSpeak(payload)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				a.fail(err)
				a.debugf("Translation error #%d: %v", payload.id, err)
				continue
			}

			totalDuration := time.Since(payload.start)
			if payload.start.IsZero() {
				totalDuration = time.Since(payload.capturedAt)
			}
			overallEstimate := payload.transcribeDuration + stats.Duration
			a.debugf(
				"Segment #%d complete partial=%t transcription=%.2fs translation=%.2fs tts_enqueue=%.2fs overall=%.2fs wall=%.2fs input=%q output=%q segments=%d chars=%d",
				payload.id,
				payload.partial,
				payload.transcribeDuration.Seconds(),
				stats.Duration.Seconds(),
				stats.EnqueueDuration.Seconds(),
				overallEstimate.Seconds(),
				totalDuration.Seconds(),
				truncate(payload.text, 240),
				truncate(stats.OutputText(), 240),
				stats.Segments,
				stats.Characters,
			)
		}
	}
}

func (a *TranslationAgent) translateAndSpeak(payload transcriptPayload) (translationStats, error) {
	var stats translationStats

	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	stream, err := a.translator.StreamTranslate(ctx, payload.text, llm.TranslateOptions{
		TargetLanguage: a.opts.TargetLanguage,
		Model:          a.opts.TranslationModel,
	})
	if err != nil {
		return stats, fmt.Errorf("translate: %w", err)
	}

	var buffer strings.Builder
	start := time.Now()

	for stream.Text != nil || stream.Err != nil {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case delta, ok := <-stream.Text:
			if !ok {
				stream.Text = nil
				continue
			}
			buffer.WriteString(delta)
			if err := a.flushBuffer(payload, &buffer, false, &stats); err != nil {
				return stats, err
			}
		case streamErr, ok := <-stream.Err:
			if !ok {
				stream.Err = nil
				continue
			}
			if streamErr != nil {
				return stats, fmt.Errorf("translate stream: %w", streamErr)
			}
		}
	}

	if err := a.flushBuffer(payload, &buffer, true, &stats); err != nil {
		return stats, err
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

func (a *TranslationAgent) flushBuffer(payload transcriptPayload, buf *strings.Builder, force bool, stats *translationStats) error {
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
				if err := a.enqueueSegment(payload, segment, stats); err != nil {
					return err
				}
			}
			start = end
		}
	}

	remainder.WriteString(strings.TrimLeft(raw[start:], " \n\t"))

	if remainder.Len() >= a.opts.FlushThreshold || (force && remainder.Len() > 0) {
		segment := strings.TrimSpace(remainder.String())
		if segment != "" {
			if err := a.enqueueSegment(payload, segment, stats); err != nil {
				return err
			}
		}
		remainder.Reset()
	}

	buf.Reset()
	buf.WriteString(remainder.String())
	return nil
}

func (a *TranslationAgent) enqueueSegment(payload transcriptPayload, segment string, stats *translationStats) error {
	enqueueStart := time.Now()
	if err := a.tts.Enqueue(segment); err != nil {
		return fmt.Errorf("tts enqueue: %w", err)
	}
	stats.EnqueueDuration += time.Since(enqueueStart)
	stats.Segments++
	stats.appendOutput(segment)
	a.debugf(
		"TTS enqueue #%d segment=%d chars=%d text=%q",
		payload.id,
		stats.Segments,
		len(segment),
		truncate(segment, 200),
	)
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

func (a *TranslationAgent) nextSequence() int64 {
	return atomic.AddInt64(&a.sequenceCounter, 1)
}

func (a *TranslationAgent) debugf(format string, args ...any) {
	log.Printf("[agent] "+format, args...)
}

func isSpeechStartEvent(eventType string) bool {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	if eventType == "" {
		return false
	}
	if strings.Contains(eventType, "speech") && (strings.Contains(eventType, "start") || strings.Contains(eventType, "begin")) {
		return true
	}
	if strings.Contains(eventType, "voice") && strings.Contains(eventType, "start") {
		return true
	}
	return false
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
