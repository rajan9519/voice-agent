package stt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// SarvamOptions define optional tuning parameters for the Sarvam provider.
type SarvamOptions struct {
	Model              string
	HighVADSensitivity bool
	VADSignals         bool
	BaseWebsocket      string
}

type sarvamProvider struct {
	apiKey  string
	opts    SarvamOptions
	conn    *websocket.Conn
	results chan TranscriptEvent
	mu      sync.Mutex
	once    sync.Once
}

// NewSarvamProvider returns a Provider backed by Sarvam's streaming API.
func NewSarvamProvider(apiKey string, opts SarvamOptions) Provider {
	if opts.BaseWebsocket == "" {
		opts.BaseWebsocket = "wss://api.sarvam.ai"
	}
	return &sarvamProvider{
		apiKey:  apiKey,
		opts:    opts,
		results: make(chan TranscriptEvent, 32),
	}
}

func (s *sarvamProvider) Connect(ctx context.Context, language string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil {
		return nil
	}

	params := url.Values{}
	if language != "" {
		params.Set("language-code", language)
	}
	if s.opts.Model != "" {
		params.Set("model", s.opts.Model)
	}
	params.Set("sample_rate", fmt.Sprintf("%d", sampleRate))
	if s.opts.HighVADSensitivity {
		params.Set("high_vad_sensitivity", "true")
	}
	if s.opts.VADSignals {
		params.Set("vad_signals", "true")
	} else {
		params.Set("vad_signals", "false")
	}

	wsURL := fmt.Sprintf("%s/speech-to-text/ws?%s", strings.TrimRight(s.opts.BaseWebsocket, "/"), params.Encode())
	headers := http.Header{}
	headers.Set("api-subscription-key", s.apiKey)
	headers.Set("User-Agent", "voiceagent-go/0.1.0")

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		return fmt.Errorf("sarvam connect: %w", err)
	}

	s.conn = conn
	go s.receiveLoop()
	return nil
}

func (s *sarvamProvider) SendAudio(ctx context.Context, pcm []byte) error {
	payload := map[string]any{
		"audio": map[string]any{
			"data":        base64.StdEncoding.EncodeToString(pcm),
			"sample_rate": sampleRate,
			"encoding":    "audio/wav",
		},
	}

	return s.writeJSON(ctx, payload)
}

func (s *sarvamProvider) Finalize(ctx context.Context) error {
	return nil
}

func (s *sarvamProvider) Results() <-chan TranscriptEvent {
	return s.results
}

func (s *sarvamProvider) Close(ctx context.Context) error {
	s.once.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
	})
	return nil
}

func (s *sarvamProvider) receiveLoop() {
	defer s.Close(context.Background())
	defer close(s.results)

	for {
		_, payload, err := s.conn.ReadMessage()
		if err != nil {
			if isNormalSocketClose(err) {
				return
			}
			s.results <- TranscriptEvent{
				Provider: "sarvam",
				Type:     "error",
				Err:      err,
			}
			return
		}

		var msg struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}

		if err := json.Unmarshal(payload, &msg); err != nil {
			s.results <- TranscriptEvent{
				Provider: "sarvam",
				Type:     "decode_error",
				Err:      err,
				Raw:      json.RawMessage(payload),
			}
			continue
		}

		if msg.Type == "data" {
			var data struct {
				Transcript   string `json:"transcript"`
				IsFinal      bool   `json:"is_final"`
				Event        string `json:"event"`
				RequestID    string `json:"request_id"`
				LanguageCode string `json:"language_code"`
			}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				s.results <- TranscriptEvent{
					Provider: "sarvam",
					Type:     "decode_error",
					Err:      err,
					Raw:      msg.Data,
				}
				continue
			}
			if data.Transcript != "" {
				s.results <- TranscriptEvent{
					Provider: "sarvam",
					Type:     "transcript",
					Text:     data.Transcript,
					IsFinal:  true,
					Raw:      msg.Data,
				}
			}
			continue
		}

		if msg.Type == "event" {
			var event struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				s.results <- TranscriptEvent{
					Provider: "sarvam",
					Type:     "decode_error",
					Err:      err,
					Raw:      msg.Data,
				}
				continue
			}
			s.results <- TranscriptEvent{
				Provider: "sarvam",
				Type:     event.Name,
				Raw:      msg.Data,
			}
			continue
		}

		if msg.Type == "error" {
			var data struct {
				Message string `json:"message"`
			}
			_ = json.Unmarshal(msg.Data, &data)
			s.results <- TranscriptEvent{
				Provider: "sarvam",
				Type:     "error",
				Err:      errors.New(data.Message),
				Raw:      msg.Data,
			}
			return
		}
	}
}

func (s *sarvamProvider) writeJSON(ctx context.Context, payload any) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return errSarvamNotConnected
	}

	deadline := time.Now().Add(websocketWriteTimeout)
	if ctx != nil {
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
			deadline = dl
		}
	}

	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer s.conn.SetWriteDeadline(time.Time{})

	return s.conn.WriteJSON(payload)
}
