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

// Converser captures the subset of the LLM client used by the conversation agent.
type Converser interface {
	StreamConverse(ctx context.Context, text string, opts llm.ConversationOptions) (*llm.StreamResponse, error)
}

// ConversationOptions configures the behaviour of the conversation agent.
type ConversationOptions struct {
	ConversationModel       string
	FlushThreshold          int
	TranscriptQueueCapacity int
	UsePartialTranscripts   bool
	// EventCallback, if set, is called for each transcript event received from STT.
	EventCallback func(eventType string, text string, isFinal bool)
}

// ConversationAgent wires STT, LLM conversation, and TTS playback into a single pipeline.
// It replies in the same language the user spoke, maintaining full conversation history.
type ConversationAgent struct {
	stt      *stt.SpeechToText
	tts      *tts.TextToSpeech
	converser Converser
	player   AudioPlayer
	opts     ConversationOptions

	ctx    context.Context
	cancel context.CancelFunc

	transcripts chan transcriptPayload
	wg          sync.WaitGroup

	errOnce sync.Once
	err     error

	sequenceCounter int64

	historyMu sync.Mutex
	history   []llm.Message
}

// NewConversationAgent validates dependencies and returns a ready-to-start agent.
func NewConversationAgent(
	sttPipeline *stt.SpeechToText,
	ttsEngine *tts.TextToSpeech,
	converser Converser,
	player AudioPlayer,
	opts ConversationOptions,
) (*ConversationAgent, error) {
	if sttPipeline == nil {
		return nil, errors.New("conversation_agent: speech-to-text pipeline is required")
	}
	if ttsEngine == nil {
		return nil, errors.New("conversation_agent: text-to-speech engine is required")
	}
	if converser == nil {
		return nil, errors.New("conversation_agent: converser client is required")
	}
	if player == nil {
		return nil, errors.New("conversation_agent: audio player is required")
	}

	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = defaultFlushThreshold
	}
	if opts.TranscriptQueueCapacity <= 0 {
		opts.TranscriptQueueCapacity = defaultTranscriptBuffer
	}

	return &ConversationAgent{
		stt:       sttPipeline,
		tts:       ttsEngine,
		converser: converser,
		player:    player,
		opts:      opts,
	}, nil
}

// Start launches background processing and begins streaming audio and transcripts.
func (a *ConversationAgent) Start(parent context.Context) error {
	if a.ctx != nil {
		return errors.New("conversation_agent: already started")
	}
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	a.ctx = ctx
	a.cancel = cancel
	a.transcripts = make(chan transcriptPayload, a.opts.TranscriptQueueCapacity)

	if err := a.tts.Start(ctx); err != nil {
		a.resetConv()
		return fmt.Errorf("conversation_agent: start tts: %w", err)
	}

	if err := a.stt.Start(ctx); err != nil {
		_ = a.tts.Stop(context.Background())
		a.resetConv()
		return fmt.Errorf("conversation_agent: start stt: %w", err)
	}

	a.wg.Add(3)
	go a.convPlaybackLoop()
	go a.convTranscriptLoop()
	go a.convLoop()

	return nil
}

// Stop cancels background work and releases resources.
func (a *ConversationAgent) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if a.ctx == nil {
		if a.player != nil {
			if err := a.player.Close(); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("conversation_agent: close player: %w", err)
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
		errs = append(errs, fmt.Errorf("conversation_agent: stop stt: %w", err))
	}
	if err := a.tts.Stop(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		errs = append(errs, fmt.Errorf("conversation_agent: stop tts: %w", err))
	}
	if err := a.player.Close(); err != nil && !errors.Is(err, context.Canceled) {
		errs = append(errs, fmt.Errorf("conversation_agent: close player: %w", err))
	}

	a.resetConv()

	if len(errs) > 0 {
		errs = append(errs, a.err)
		return errors.Join(errs...)
	}

	return a.err
}

// Err returns the first asynchronous error encountered by the agent, if any.
func (a *ConversationAgent) Err() error {
	return a.err
}

func (a *ConversationAgent) resetConv() {
	a.ctx = nil
	a.cancel = nil
	a.transcripts = nil
}

func (a *ConversationAgent) convPlaybackLoop() {
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
				a.convFail(fmt.Errorf("playback: %w", err))
				return
			}
		}
	}
}

func (a *ConversationAgent) convTranscriptLoop() {
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
				a.convDebugf("STT provider error: %v", event.Err)
				a.convFail(fmt.Errorf("stt provider: %w", event.Err))
				continue
			}

			now := time.Now()

			if event.Type == "transcript" {
				if !event.IsFinal {
					if utteranceStart.IsZero() {
						utteranceStart = now
					}
					if currentSeq == -1 {
						currentSeq = a.convNextSequence()
					}

					text := strings.TrimSpace(event.Text)
					if !a.opts.UsePartialTranscripts {
						if text != "" {
							a.convDebugf("STT partial ignored (final-only mode): %q", truncate(text, 120))
						}
						continue
					}
					if text == "" {
						continue
					}

					elapsed := now.Sub(utteranceStart)
					a.convDebugf(
						"STT partial #%d provider=%s elapsed=%.2fs text=%q",
						currentSeq, event.Provider, elapsed.Seconds(), truncate(text, 160),
					)

					if a.opts.EventCallback != nil {
						a.opts.EventCallback("transcript", text, false)
					}

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
					currentSeq = a.convNextSequence()
				}

				duration := now.Sub(utteranceStart)
				seq := currentSeq
				if seq == -1 {
					seq = a.convNextSequence()
				}

				a.convDebugf(
					"STT final #%d provider=%s duration=%.2fs text=%q",
					seq, event.Provider, duration.Seconds(), truncate(text, 240),
				)

				if a.opts.EventCallback != nil {
					a.opts.EventCallback("transcript", text, true)
				}

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
					currentSeq = a.convNextSequence()
				}
			}

			text := strings.TrimSpace(event.Text)
			if text == "" {
				a.convDebugf("STT event %q", event.Type)
				continue
			}
			a.convDebugf("STT event %q: %s", event.Type, truncate(text, 120))

		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if err != nil {
				a.convDebugf("Recorder error: %v", err)
				a.convFail(fmt.Errorf("recorder: %w", err))
			}
		}
	}
}

func (a *ConversationAgent) convLoop() {
	defer a.wg.Done()

	for {
		select {
		case <-a.ctx.Done():
			return
		case payload, ok := <-a.transcripts:
			if !ok {
				return
			}

			a.convDebugf(
				"Conversation start #%d partial=%t text=%q",
				payload.id, payload.partial, truncate(payload.text, 200),
			)

			stats, err := a.converseAndSpeak(payload)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				a.convFail(err)
				a.convDebugf("Conversation error #%d: %v", payload.id, err)
				continue
			}

			totalDuration := time.Since(payload.start)
			if payload.start.IsZero() {
				totalDuration = time.Since(payload.capturedAt)
			}
			overallEstimate := payload.transcribeDuration + stats.Duration
			a.convDebugf(
				"Segment #%d complete partial=%t transcription=%.2fs reply=%.2fs tts_enqueue=%.2fs overall=%.2fs wall=%.2fs input=%q output=%q segments=%d chars=%d",
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

func (a *ConversationAgent) converseAndSpeak(payload transcriptPayload) (translationStats, error) {
	var stats translationStats

	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	a.historyMu.Lock()
	history := make([]llm.Message, len(a.history))
	copy(history, a.history)
	a.historyMu.Unlock()

	stream, err := a.converser.StreamConverse(ctx, payload.text, llm.ConversationOptions{
		Model:   a.opts.ConversationModel,
		History: history,
	})
	if err != nil {
		return stats, fmt.Errorf("converse: %w", err)
	}

	var (
		buffer  strings.Builder
		replyBuf strings.Builder
	)
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
			replyBuf.WriteString(delta)
			if err := a.convFlushBuffer(payload, &buffer, false, &stats); err != nil {
				return stats, err
			}
		case streamErr, ok := <-stream.Err:
			if !ok {
				stream.Err = nil
				continue
			}
			if streamErr != nil {
				return stats, fmt.Errorf("converse stream: %w", streamErr)
			}
		}
	}

	if err := a.convFlushBuffer(payload, &buffer, true, &stats); err != nil {
		return stats, err
	}

	stats.Duration = time.Since(start)

	// Only update history on final (non-partial) turns to avoid duplicate context.
	if !payload.partial {
		reply := strings.TrimSpace(replyBuf.String())
		if reply != "" {
			if a.opts.EventCallback != nil {
				a.opts.EventCallback("reply", reply, true)
			}
			a.historyMu.Lock()
			a.history = append(a.history,
				llm.Message{
					Role: "user",
					Content: []llm.MessageContent{
						{Type: "input_text", Text: payload.text},
					},
				},
				llm.Message{
					Role: "assistant",
					Content: []llm.MessageContent{
						{Type: "input_text", Text: reply},
					},
				},
			)
			a.historyMu.Unlock()
		}
	}

	return stats, nil
}

func (a *ConversationAgent) convFlushBuffer(payload transcriptPayload, buf *strings.Builder, force bool, stats *translationStats) error {
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
				if err := a.convEnqueueSegment(payload, segment, stats); err != nil {
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
			if err := a.convEnqueueSegment(payload, segment, stats); err != nil {
				return err
			}
		}
		remainder.Reset()
	}

	buf.Reset()
	buf.WriteString(remainder.String())
	return nil
}

func (a *ConversationAgent) convEnqueueSegment(payload transcriptPayload, segment string, stats *translationStats) error {
	enqueueStart := time.Now()
	if err := a.tts.Enqueue(segment); err != nil {
		return fmt.Errorf("tts enqueue: %w", err)
	}
	stats.EnqueueDuration += time.Since(enqueueStart)
	stats.Segments++
	stats.appendOutput(segment)
	a.convDebugf(
		"TTS enqueue #%d segment=%d chars=%d text=%q",
		payload.id, stats.Segments, len(segment), truncate(segment, 200),
	)
	return nil
}

func (a *ConversationAgent) convFail(err error) {
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

func (a *ConversationAgent) convNextSequence() int64 {
	return atomic.AddInt64(&a.sequenceCounter, 1)
}

func (a *ConversationAgent) convDebugf(format string, args ...any) {
	log.Printf("[conversation_agent] "+format, args...)
}
