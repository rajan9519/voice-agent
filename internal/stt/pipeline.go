package stt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Options configures a SpeechToText pipeline instance.
type Options struct {
	Language    string
	ErrorBuffer int
}

// SpeechToText coordinates microphone capture, provider streaming, and event propagation.
type SpeechToText struct {
	provider Provider
	recorder Recorder
	language string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	results <-chan TranscriptEvent
	errors  chan error

	errOnce sync.Once
	err     error

	finalizeOnce sync.Once
}

// New constructs a SpeechToText pipeline.
func New(provider Provider, recorder Recorder, opts Options) (*SpeechToText, error) {
	if provider == nil {
		return nil, errors.New("stt: provider is required")
	}
	if recorder == nil {
		return nil, errors.New("stt: recorder is required")
	}

	errorDepth := opts.ErrorBuffer
	if errorDepth <= 0 {
		errorDepth = 4
	}

	return &SpeechToText{
		provider: provider,
		recorder: recorder,
		language: opts.Language,
		errors:   make(chan error, errorDepth),
	}, nil
}

// Start opens provider connections and begins background streaming.
func (s *SpeechToText) Start(parent context.Context) error {
	if s.ctx != nil {
		return errors.New("stt: pipeline already started")
	}
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	s.ctx = ctx
	s.cancel = cancel

	if err := s.provider.Connect(ctx, s.language); err != nil {
		cancel()
		s.ctx = nil
		return fmt.Errorf("connect provider: %w", err)
	}

	audioCh, audioErrCh, err := s.recorder.Start(ctx)
	if err != nil {
		cancel()
		s.ctx = nil
		_ = s.provider.Close(context.Background())
		_ = s.recorder.Close()
		return fmt.Errorf("start recorder: %w", err)
	}

	s.results = s.provider.Results()

	s.wg.Add(2)
	go s.streamAudio(audioCh)
	go s.pipeAudioErrors(audioErrCh)

	return nil
}

// Results exposes provider transcripts.
func (s *SpeechToText) Results() <-chan TranscriptEvent {
	return s.results
}

// Errors emits asynchronous pipeline failures (microphone, streaming, etc).
func (s *SpeechToText) Errors() <-chan error {
	return s.errors
}

// Stop halts background work and releases all resources.
func (s *SpeechToText) Stop(ctx context.Context) error {
	if s.ctx == nil {
		return nil
	}

	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	close(s.errors)

	var err error
	if closeErr := s.provider.Close(context.Background()); closeErr != nil && !errors.Is(closeErr, io.EOF) {
		err = closeErr
	}
	if recErr := s.recorder.Close(); recErr != nil && err == nil {
		err = recErr
	}
	if err == nil {
		err = s.err
	}

	s.ctx = nil
	s.cancel = nil
	return err
}

func (s *SpeechToText) streamAudio(ch <-chan AudioChunk) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			s.finalize()
			return
		case chunk, ok := <-ch:
			if !ok {
				s.finalize()
				return
			}
			data := chunk.Bytes()
			if len(data) == 0 {
				chunk.Release()
				continue
			}
			if err := s.provider.SendAudio(s.ctx, data); err != nil {
				chunk.Release()
				s.captureErr(fmt.Errorf("stream audio: %w", err))
				return
			}
			chunk.Release()
		}
	}
}

func (s *SpeechToText) pipeAudioErrors(ch <-chan error) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case err, ok := <-ch:
			if !ok {
				return
			}
			if err != nil {
				s.captureErr(fmt.Errorf("recorder: %w", err))
			}
		}
	}
}

func (s *SpeechToText) finalize() {
	s.finalizeOnce.Do(func() {
		if err := s.provider.Finalize(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.captureErr(fmt.Errorf("finalize: %w", err))
		}
	})
}

func (s *SpeechToText) captureErr(err error) {
	if err == nil {
		return
	}
	select {
	case s.errors <- err:
	default:
	}
	s.errOnce.Do(func() {
		s.err = err
	})
}
