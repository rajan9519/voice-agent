package stt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// CartesiaOptions define optional tuning parameters for the Cartesia provider.
type CartesiaOptions struct {
	Model         string
	Encoding      string
	MinVolume     *float64
	MaxSilence    time.Duration
	APIVersion    string
	BaseWebsocket string
}

type cartesiaProvider struct {
	apiKey  string
	opts    CartesiaOptions
	conn    *websocket.Conn
	results chan TranscriptEvent
	mu      sync.Mutex
	once    sync.Once
}

// NewCartesiaProvider returns a Provider backed by Cartesia's streaming API.
func NewCartesiaProvider(apiKey string, opts CartesiaOptions) Provider {
	if opts.Model == "" {
		opts.Model = "ink-whisper"
	}
	if opts.Encoding == "" {
		opts.Encoding = "pcm_s16le"
	}
	if opts.APIVersion == "" {
		opts.APIVersion = "2024-11-13"
	}
	if opts.BaseWebsocket == "" {
		opts.BaseWebsocket = "wss://api.cartesia.ai"
	}
	if opts.MinVolume == nil {
		defaultVolume := 0.15
		opts.MinVolume = &defaultVolume
	}
	if opts.MaxSilence == 0 {
		opts.MaxSilence = 800 * time.Millisecond
	}

	return &cartesiaProvider{
		apiKey:  apiKey,
		opts:    opts,
		results: make(chan TranscriptEvent, 32),
	}
}

func (c *cartesiaProvider) Connect(ctx context.Context, language string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	params := url.Values{}
	params.Set("model", c.opts.Model)
	params.Set("api_key", c.apiKey)
	params.Set("cartesia_version", c.opts.APIVersion)
	params.Set("encoding", c.opts.Encoding)
	params.Set("sample_rate", fmt.Sprintf("%d", sampleRate))
	if language != "" {
		params.Set("language", language)
	}
	if c.opts.MinVolume != nil {
		params.Set("min_volume", fmt.Sprintf("%.2f", *c.opts.MinVolume))
	}
	if c.opts.MaxSilence > 0 {
		params.Set("max_silence_duration_secs", fmt.Sprintf("%.2f", c.opts.MaxSilence.Seconds()))
	}

	wsURL := fmt.Sprintf("%s/stt/websocket?%s", strings.TrimRight(c.opts.BaseWebsocket, "/"), params.Encode())
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("cartesia connect: %w", err)
	}
	c.conn = conn
	go c.receiveLoop()
	return nil
}

func (c *cartesiaProvider) SendAudio(ctx context.Context, pcm []byte) error {
	return c.writeMessage(ctx, websocket.BinaryMessage, pcm)
}

func (c *cartesiaProvider) Finalize(ctx context.Context) error {
	if err := c.writeMessage(ctx, websocket.TextMessage, []byte("finalize")); err != nil {
		if errors.Is(err, errCartesiaNotConnected) {
			return nil
		}
		return err
	}
	if err := c.writeMessage(ctx, websocket.TextMessage, []byte("done")); err != nil {
		if errors.Is(err, errCartesiaNotConnected) {
			return nil
		}
		return err
	}
	return nil
}

func (c *cartesiaProvider) writeMessage(ctx context.Context, messageType int, data []byte) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return errCartesiaNotConnected
	}

	deadline := time.Now().Add(websocketWriteTimeout)
	if ctx != nil {
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
			deadline = dl
		}
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})

	if err := c.conn.WriteMessage(messageType, data); err != nil {
		return err
	}

	return nil
}

func (c *cartesiaProvider) Results() <-chan TranscriptEvent {
	return c.results
}

func (c *cartesiaProvider) Close(ctx context.Context) error {
	c.once.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
	})
	return nil
}

func (c *cartesiaProvider) receiveLoop() {
	defer c.Close(context.Background())
	defer close(c.results)

	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			if isNormalSocketClose(err) {
				return
			}
			c.results <- TranscriptEvent{
				Provider: "cartesia",
				Type:     "error",
				Err:      err,
			}
			return
		}

		var msg struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			IsFinal bool   `json:"is_final"`
		}

		if err := json.Unmarshal(payload, &msg); err != nil {
			c.results <- TranscriptEvent{
				Provider: "cartesia",
				Type:     "decode_error",
				Err:      err,
				Raw:      json.RawMessage(payload),
			}
			continue
		}

		event := TranscriptEvent{
			Provider: "cartesia",
			Type:     msg.Type,
			Text:     msg.Text,
			IsFinal:  msg.IsFinal,
			Raw:      json.RawMessage(payload),
		}

		c.results <- event

		if msg.Type == "done" {
			return
		}
	}
}
