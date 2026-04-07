package ws

import (
	"encoding/binary"
	"math"
	"sync"

	"voiceagent/internal/tts"
)

// WebSocketPlayer implements agent.AudioPlayer by forwarding PCM audio chunks
// to the session's write pump as binary WebSocket messages.
type WebSocketPlayer struct {
	sendBinary func([]byte) error
	once       sync.Once
}

// NewWebSocketPlayer creates a player that encodes TTS AudioChunks as little-endian
// float32 PCM bytes and passes them to sendBinary for transmission over the WebSocket.
func NewWebSocketPlayer(sendBinary func([]byte) error) *WebSocketPlayer {
	return &WebSocketPlayer{sendBinary: sendBinary}
}

// Play converts float32 samples to LE bytes and sends them to the client.
// EndOfSegment markers are ignored (the client uses the stream continuously).
func (p *WebSocketPlayer) Play(chunk tts.AudioChunk) error {
	if chunk.EndOfSegment || len(chunk.Samples) == 0 {
		return nil
	}
	buf := make([]byte, len(chunk.Samples)*4)
	for i, s := range chunk.Samples {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(s))
	}
	return p.sendBinary(buf)
}

// Close is a no-op; the session owns the WebSocket lifecycle.
func (p *WebSocketPlayer) Close() error {
	return nil
}
