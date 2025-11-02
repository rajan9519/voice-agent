package stt

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/gordonklaus/portaudio"
)

const (
	sampleRate = 16000
	chunkSize  = 1600
	channels   = 1
)

// AudioChunk wraps an encoded PCM payload captured from the microphone.
type AudioChunk struct {
	data []byte
	pool *sync.Pool
}

// Bytes exposes the chunk PCM data.
func (c AudioChunk) Bytes() []byte {
	return c.data
}

// Release returns the underlying buffer to the pool.
func (c AudioChunk) Release() {
	if c.pool == nil || c.data == nil {
		return
	}
	c.pool.Put(c.data[:cap(c.data)])
}

// Recorder emits PCM audio suitable for streaming to the provider.
type Recorder interface {
	Start(ctx context.Context) (<-chan AudioChunk, <-chan error, error)
	Close() error
}

// PortAudioRecorder captures microphone samples using PortAudio.
type PortAudioRecorder struct {
	stream *portaudio.Stream
	buffer []int16
	pool   *sync.Pool
	once   sync.Once
}

// NewPortAudioRecorder constructs a Recorder that reads from the default input device.
func NewPortAudioRecorder() (*PortAudioRecorder, error) {
	if err := portaudio.Initialize(); err != nil {
		return nil, err
	}
	buf := make([]int16, chunkSize)
	stream, err := portaudio.OpenDefaultStream(channels, 0, float64(sampleRate), len(buf), &buf)
	if err != nil {
		_ = portaudio.Terminate()
		return nil, err
	}
	return &PortAudioRecorder{
		stream: stream,
		buffer: buf,
		pool: &sync.Pool{
			New: func() any {
				return make([]byte, len(buf)*2)
			},
		},
	}, nil
}

// Start begins capturing audio and returns channels for streaming bytes and async errors.
func (r *PortAudioRecorder) Start(ctx context.Context) (<-chan AudioChunk, <-chan error, error) {
	if err := r.stream.Start(); err != nil {
		return nil, nil, err
	}

	dataCh := make(chan AudioChunk, 4)
	errCh := make(chan error, 1)

	go func() {
		defer close(dataCh)
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := r.stream.Read(); err != nil {
				if errors.Is(err, portaudio.InputOverflowed) {
					continue
				}
				errCh <- err
				return
			}

			raw := r.pool.Get()
			var chunk []byte
			switch v := raw.(type) {
			case []byte:
				if cap(v) < len(r.buffer)*2 {
					v = make([]byte, len(r.buffer)*2)
				}
				chunk = v[:len(r.buffer)*2]
			default:
				chunk = make([]byte, len(r.buffer)*2)
			}

			for i, sample := range r.buffer {
				binary.LittleEndian.PutUint16(chunk[i*2:], uint16(sample))
			}

			select {
			case dataCh <- AudioChunk{data: chunk, pool: r.pool}:
			case <-ctx.Done():
				r.pool.Put(chunk[:cap(chunk)])
				return
			}
		}
	}()

	return dataCh, errCh, nil
}

// Close stops the stream and releases underlying PortAudio resources.
func (r *PortAudioRecorder) Close() error {
	var err error
	r.once.Do(func() {
		if r.stream != nil {
			if stopErr := r.stream.Stop(); stopErr != nil && err == nil {
				err = stopErr
			}
			if closeErr := r.stream.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
		_ = portaudio.Terminate()
	})
	return err
}
