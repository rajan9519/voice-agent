package tts

import "context"

// SynthesisRequest encapsulates the parameters for a TTS synthesis call.
type SynthesisRequest struct {
	Text       string
	VoiceID    string
	ModelID    string
	SampleRate int
	Language   string
}

// AudioChunk represents a decoded PCM payload ready for playback.
type AudioChunk struct {
	Samples      []float32
	EndOfSegment bool
}

// Provider defines the minimal functionality a TTS vendor implementation
// must expose so the pipeline can remain provider-agnostic.
type Provider interface {
	Connect(ctx context.Context) error
	Stream(ctx context.Context, req SynthesisRequest) (<-chan AudioChunk, <-chan error, error)
	Close(ctx context.Context) error
}
