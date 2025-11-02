package tts

import (
	"errors"
	"sync"

	"github.com/gordonklaus/portaudio"
)

const (
	defaultFramesPerBuffer = 2048
)

// PortAudioPlayer wraps the PortAudio stream used for playback.
type PortAudioPlayer struct {
	stream  *portaudio.Stream
	buffer  []float32
	pending []float32
	started bool
	once    sync.Once
}

// NewPortAudioPlayer creates a speaker output using PortAudio.
func NewPortAudioPlayer(sampleRate int) (*PortAudioPlayer, error) {
	if err := portaudio.Initialize(); err != nil {
		return nil, err
	}

	buf := make([]float32, defaultFramesPerBuffer)
	stream, err := portaudio.OpenDefaultStream(0, 1, float64(sampleRate), len(buf), &buf)
	if err != nil {
		_ = portaudio.Terminate()
		return nil, err
	}

	return &PortAudioPlayer{stream: stream, buffer: buf}, nil
}

func (p *PortAudioPlayer) start() error {
	if p.stream == nil {
		return errors.New("audio stream not initialized")
	}
	if p.started {
		return nil
	}
	if err := p.stream.Start(); err != nil {
		return err
	}
	p.started = true
	return nil
}

// Close releases the underlying PortAudio resources.
func (p *PortAudioPlayer) Close() error {
	var err error
	p.once.Do(func() {
		if len(p.pending) > 0 && err == nil {
			err = p.flush()
		}
		if p.stream != nil {
			if p.started {
				if stopErr := p.stream.Stop(); stopErr != nil && err == nil {
					err = stopErr
				}
			}
			if closeErr := p.stream.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			p.stream = nil
		}
		_ = portaudio.Terminate()
	})
	return err
}

// Play sends the provided chunk to the speaker.
func (p *PortAudioPlayer) Play(chunk AudioChunk) error {
	if chunk.EndOfSegment {
		return p.flush()
	}
	return p.write(chunk.Samples)
}

func (p *PortAudioPlayer) write(samples []float32) error {
	if len(samples) == 0 {
		return nil
	}
	if err := p.start(); err != nil {
		return err
	}

	p.pending = append(p.pending, samples...)
	frames := len(p.buffer)
	for len(p.pending) >= frames {
		copy(p.buffer, p.pending[:frames])
		if err := p.stream.Write(); err != nil {
			if errors.Is(err, portaudio.OutputUnderflowed) {
				p.pending = p.pending[frames:]
				continue
			}
			return err
		}
		p.pending = p.pending[frames:]
	}
	return nil
}

func (p *PortAudioPlayer) flush() error {
	if len(p.pending) == 0 {
		return nil
	}
	if err := p.start(); err != nil {
		return err
	}
	copy(p.buffer, p.pending)
	for i := len(p.pending); i < len(p.buffer); i++ {
		p.buffer[i] = 0
	}
	if err := p.stream.Write(); err != nil {
		if errors.Is(err, portaudio.OutputUnderflowed) {
			p.pending = p.pending[:0]
			return nil
		}
		return err
	}
	p.pending = p.pending[:0]
	return nil
}
