package tts

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// TextToSpeech coordinates provider streaming, queuing, and playback.
type TextToSpeech struct {
	provider   Provider
	sampleRate int
	voiceID    string
	modelID    string
	language   string

	textQueue  chan textRequest
	audioQueue chan AudioChunk

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type textRequest struct {
	text string
	done chan error
}

// Options configures a TextToSpeech instance.
type Options struct {
	SampleRate      int
	VoiceID         string
	ModelID         string
	Language        string
	TextQueueDepth  int
	AudioQueueDepth int
	FramesPerBuffer int
}

// New constructs TextToSpeech with the provided provider abstraction.
func New(provider Provider, opts Options) (*TextToSpeech, error) {
	if opts.SampleRate <= 0 {
		return nil, fmt.Errorf("invalid sample rate %d", opts.SampleRate)
	}

	textDepth := opts.TextQueueDepth
	if textDepth <= 0 {
		textDepth = 8
	}
	audioDepth := opts.AudioQueueDepth
	if audioDepth <= 0 {
		audioDepth = 32
	}

	return &TextToSpeech{
		provider:   provider,
		sampleRate: opts.SampleRate,
		voiceID:    opts.VoiceID,
		modelID:    opts.ModelID,
		language:   opts.Language,
		textQueue:  make(chan textRequest, textDepth),
		audioQueue: make(chan AudioChunk, audioDepth),
	}, nil
}

// Start begins provider connectivity and background workers.
func (t *TextToSpeech) Start(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	if err := t.provider.Connect(parent); err != nil {
		return fmt.Errorf("connect provider: %w", err)
	}

	t.ctx, t.cancel = context.WithCancel(parent)

	t.wg.Add(1)
	go t.textLoop()

	return nil
}

// AudioStream exposes the decoded PCM chunks produced by the provider.
func (t *TextToSpeech) AudioStream() <-chan AudioChunk {
	return t.audioQueue
}

// Speak synthesizes text and blocks until the provider finishes streaming.
func (t *TextToSpeech) Speak(ctx context.Context, text string) error {
	if ctx == nil {
		ctx = t.ctx
		if ctx == nil {
			ctx = context.Background()
		}
	}
	done := make(chan error, 1)
	select {
	case t.textQueue <- textRequest{text: text, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Enqueue schedules text for synthesis without waiting for completion.
func (t *TextToSpeech) Enqueue(text string) error {
	if t.ctx == nil {
		return errors.New("pipeline not started")
	}
	select {
	case t.textQueue <- textRequest{text: text}:
		return nil
	case <-t.ctx.Done():
		return t.ctx.Err()
	}
}

// Stop halts background work and releases all resources.
func (t *TextToSpeech) Stop(ctx context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}

	close(t.textQueue)

	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	var err error
	if closeErr := t.provider.Close(context.Background()); closeErr != nil && !errors.Is(closeErr, io.EOF) {
		err = closeErr
	}

	return err
}

func (t *TextToSpeech) textLoop() {
	defer t.wg.Done()
	defer close(t.audioQueue)

	for {
		select {
		case <-t.ctx.Done():
			return
		case req, ok := <-t.textQueue:
			if !ok {
				return
			}
			if req.text == "" {
				if req.done != nil {
					req.done <- fmt.Errorf("empty text")
				}
				continue
			}
			if err := t.handleRequest(req); err != nil && req.done != nil {
				req.done <- err
			} else if req.done != nil {
				req.done <- nil
			}
		}
	}
}

func (t *TextToSpeech) handleRequest(req textRequest) error {
	streamCtx, cancel := context.WithCancel(t.ctx)
	defer cancel()

	audioCh, errCh, err := t.provider.Stream(streamCtx, SynthesisRequest{
		Text:       req.text,
		VoiceID:    t.voiceID,
		ModelID:    t.modelID,
		SampleRate: t.sampleRate,
		Language:   t.language,
	})
	if err != nil {
		return err
	}

	for {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		case err := <-errCh:
			if err != nil {
				return err
			}
			return nil
		case chunk, ok := <-audioCh:
			if !ok {
				// provider finished without error
				select {
				case t.audioQueue <- AudioChunk{EndOfSegment: true}:
				case <-t.ctx.Done():
				}
				return nil
			}
			select {
			case t.audioQueue <- chunk:
			case <-t.ctx.Done():
				return t.ctx.Err()
			}
		}
	}
}

// DrainScanner feeds lines from stdin into the pipeline until EOF or context cancellation.
func (t *TextToSpeech) DrainScanner(ctx context.Context, scanner *bufio.Scanner) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			return io.EOF
		}

		line := scanner.Text()
		if line == "" {
			continue
		}
		if err := t.Enqueue(line); err != nil {
			return err
		}
	}
}
