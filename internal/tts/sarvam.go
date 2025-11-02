package tts

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultSarvamBaseURL = "wss://api.sarvam.ai"
	sarvamReadTimeout    = 15 * time.Second
)

// SarvamProvider implements Provider for Sarvam AI's streaming TTS.
type SarvamProvider struct {
	apiKey     string
	modelID    string
	language   string
	sampleRate int
	baseURL    string
}

type sarvamEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type sarvamAudioData struct {
	ContentType string `json:"content_type"`
	Audio       string `json:"audio"`
}

type sarvamEventData struct {
	EventType string `json:"event_type"`
}

type sarvamErrorData struct {
	Message string `json:"message"`
}

// NewSarvamProvider constructs a provider with sane defaults.
func NewSarvamProvider(apiKey string, sampleRate int, language string, modelID string) *SarvamProvider {
	if modelID == "" {
		modelID = "bulbul:v2"
	}
	return &SarvamProvider{
		apiKey:     apiKey,
		modelID:    modelID,
		language:   language,
		sampleRate: sampleRate,
		baseURL:    defaultSarvamBaseURL,
	}
}

func (s *SarvamProvider) Connect(_ context.Context) error {
	if s.apiKey == "" {
		return errors.New("sarvam api key missing")
	}
	return nil
}

func (s *SarvamProvider) Stream(ctx context.Context, req SynthesisRequest) (<-chan AudioChunk, <-chan error, error) {
	headers := http.Header{}
	headers.Set("Api-Subscription-Key", s.apiKey)

	wsURL, err := s.buildWebsocketURL(req.ModelID)
	if err != nil {
		return nil, nil, err
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		return nil, nil, fmt.Errorf("sarvam dial: %w", err)
	}

	if err := s.configure(ctx, conn, req); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("sarvam configure: %w", err)
	}
	if err := s.sendText(ctx, conn, req.Text); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("sarvam send text: %w", err)
	}
	if err := s.flush(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("sarvam flush: %w", err)
	}

	audioCh := make(chan AudioChunk, 8)
	errCh := make(chan error, 1)

	go func() {
		defer close(audioCh)
		defer close(errCh)
		defer conn.Close()

		for {
			if err := conn.SetReadDeadline(time.Now().Add(sarvamReadTimeout)); err != nil {
				errCh <- err
				return
			}
			_, data, err := conn.ReadMessage()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if ctx.Err() != nil {
						errCh <- ctx.Err()
						return
					}
					continue
				}
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					errCh <- io.EOF
				} else {
					errCh <- fmt.Errorf("sarvam read: %w", err)
				}
				return
			}

			var env sarvamEnvelope
			if err := json.Unmarshal(data, &env); err != nil {
				errCh <- fmt.Errorf("sarvam decode: %w", err)
				return
			}

			switch env.Type {
			case "audio":
				var payload sarvamAudioData
				if err := json.Unmarshal(env.Data, &payload); err != nil {
					errCh <- fmt.Errorf("sarvam audio payload: %w", err)
					return
				}
				wavBytes, err := base64.StdEncoding.DecodeString(payload.Audio)
				if err != nil {
					errCh <- fmt.Errorf("sarvam base64: %w", err)
					return
				}
				samples, err := decodeWAVToFloat32(wavBytes)
				if err != nil {
					errCh <- fmt.Errorf("sarvam wav decode: %w", err)
					return
				}
				select {
				case audioCh <- AudioChunk{Samples: samples}:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case "event":
				var event sarvamEventData
				if err := json.Unmarshal(env.Data, &event); err != nil {
					errCh <- fmt.Errorf("sarvam event payload: %w", err)
					return
				}
				if event.EventType == "final" {
					return
				}
			case "error":
				var errPayload sarvamErrorData
				if err := json.Unmarshal(env.Data, &errPayload); err != nil {
					errCh <- fmt.Errorf("sarvam error payload: %w", err)
					return
				}
				errCh <- fmt.Errorf("sarvam error: %s", errPayload.Message)
				return
			default:
				// ignore ping responses, etc.
			}
		}
	}()

	return audioCh, errCh, nil
}

func (s *SarvamProvider) Close(_ context.Context) error {
	return nil
}

func (s *SarvamProvider) buildWebsocketURL(modelID string) (string, error) {
	base := s.baseURL
	if base == "" {
		base = defaultSarvamBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse sarvam base url: %w", err)
	}
	u.Path = "/text-to-speech/ws"
	q := u.Query()
	model := modelID
	if model == "" {
		model = s.modelID
	}
	q.Set("model", model)
	q.Set("send_completion_event", "true")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (s *SarvamProvider) configure(ctx context.Context, conn *websocket.Conn, req SynthesisRequest) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	language := req.Language
	if language == "" {
		language = s.language
	}
	payload := map[string]any{
		"type": "config",
		"data": map[string]any{
			"target_language_code": language,
			"speaker":              req.VoiceID,
			"speech_sample_rate":   req.SampleRate,
			"output_audio_codec":   "wav",
		},
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(payload)
}

func (s *SarvamProvider) sendText(ctx context.Context, conn *websocket.Conn, text string) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	payload := map[string]any{
		"type": "text",
		"data": map[string]any{
			"text": text,
		},
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(payload)
}

func (s *SarvamProvider) flush(ctx context.Context, conn *websocket.Conn) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	payload := map[string]any{
		"type": "flush",
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(payload)
}

func decodeWAVToFloat32(wavData []byte) ([]float32, error) {
	if len(wavData) < 44 {
		return nil, fmt.Errorf("wav payload too small: %d bytes", len(wavData))
	}
	if string(wavData[0:4]) != "RIFF" || string(wavData[8:12]) != "WAVE" {
		return nil, fmt.Errorf("unsupported wav header")
	}
	offset := 12
	var (
		numChannels   uint16 = 1
		bitsPerSample uint16 = 16
		dataStart            = -1
		dataSize      int
	)
	for offset+8 <= len(wavData) {
		chunkID := string(wavData[offset : offset+4])
		chunkLen := int(binary.LittleEndian.Uint32(wavData[offset+4 : offset+8]))
		next := offset + 8 + chunkLen
		if next > len(wavData) {
			return nil, fmt.Errorf("wav chunk %s truncated", chunkID)
		}
		switch chunkID {
		case "fmt ":
			if chunkLen < 16 {
				return nil, fmt.Errorf("wav fmt chunk too small")
			}
			audioFormat := binary.LittleEndian.Uint16(wavData[offset+8 : offset+10])
			if audioFormat != 1 {
				return nil, fmt.Errorf("unsupported wav audio format %d", audioFormat)
			}
			numChannels = binary.LittleEndian.Uint16(wavData[offset+10 : offset+12])
			bitsPerSample = binary.LittleEndian.Uint16(wavData[offset+22 : offset+24])
		case "data":
			dataStart = offset + 8
			dataSize = chunkLen
		}
		if chunkLen%2 == 1 {
			next++
		}
		offset = next
	}
	if dataStart < 0 {
		return nil, fmt.Errorf("wav data chunk missing")
	}
	if bitsPerSample != 16 {
		return nil, fmt.Errorf("unsupported wav bit depth %d", bitsPerSample)
	}
	if numChannels == 0 {
		numChannels = 1
	}
	frameSize := int(numChannels) * 2
	if frameSize <= 0 {
		return nil, fmt.Errorf("invalid wav frame size %d", frameSize)
	}
	if dataStart+dataSize > len(wavData) {
		dataSize = len(wavData) - dataStart
	}
	frames := dataSize / frameSize
	if frames == 0 {
		return nil, fmt.Errorf("wav data empty")
	}
	pcm := wavData[dataStart : dataStart+frames*frameSize]
	samples := make([]float32, frames)
	for i := 0; i < frames; i++ {
		sample := int16(binary.LittleEndian.Uint16(pcm[i*frameSize:]))
		samples[i] = float32(sample) / 32768.0
	}
	return samples, nil
}
