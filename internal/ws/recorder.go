package ws

import (
	"context"
	"sync"

	"voiceagent/internal/stt"
)

// WebSocketRecorder implements stt.Recorder by reading PCM audio from a channel
// fed by the WebSocket session's read pump.
type WebSocketRecorder struct {
	audioCh chan stt.AudioChunk
	once    sync.Once
}

// NewWebSocketRecorder creates a recorder whose audio comes from the returned feed channel.
// The caller (session read pump) pushes stt.AudioChunk values into the channel;
// the STT pipeline consumes them via Start().
func NewWebSocketRecorder() (*WebSocketRecorder, chan<- stt.AudioChunk) {
	ch := make(chan stt.AudioChunk, 8)
	return &WebSocketRecorder{audioCh: ch}, ch
}

// Start returns the audio channel and a nil-error channel (errors come via session, not recorder).
func (r *WebSocketRecorder) Start(_ context.Context) (<-chan stt.AudioChunk, <-chan error, error) {
	errCh := make(chan error, 1)
	// errCh stays open; session closing audioCh signals end of input.
	return r.audioCh, errCh, nil
}

// Close is a no-op; the session manages the channel lifecycle.
func (r *WebSocketRecorder) Close() error {
	r.once.Do(func() {
		// Closing audioCh signals the STT pipeline that no more audio is coming.
		close(r.audioCh)
	})
	return nil
}
