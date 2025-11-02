package stt

import (
	"context"
	"encoding/json"
)

// TranscriptEvent represents a message emitted by a speech provider.
type TranscriptEvent struct {
	Provider string
	Type     string
	Text     string
	IsFinal  bool
	Raw      json.RawMessage
	Err      error
}

// Provider abstracts streaming speech-to-text vendors.
type Provider interface {
	Connect(ctx context.Context, language string) error
	SendAudio(ctx context.Context, pcm []byte) error
	Finalize(ctx context.Context) error
	Results() <-chan TranscriptEvent
	Close(ctx context.Context) error
}
