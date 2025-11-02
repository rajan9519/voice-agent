package tts

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	defaultCartesiaBaseURL    = "wss://api.cartesia.ai"
	defaultCartesiaAPIVersion = "2024-11-13"
	cartesiaReadTimeout       = 15 * time.Second
)

// CartesiaProvider implements the Provider interface for Cartesia's TTS websocket.
type CartesiaProvider struct {
	apiKey     string
	modelID    string
	sampleRate int
	baseURL    string
	apiVersion string

	dialer *websocket.Dialer

	mu     sync.Mutex
	conn   *websocket.Conn
	active bool
}

type cartesiaResponse struct {
	Type      string `json:"type"`
	Data      string `json:"data"`
	Error     string `json:"error"`
	ContextID string `json:"context_id"`
}

// NewCartesiaProvider constructs a Cartesia provider.
func NewCartesiaProvider(apiKey string, sampleRate int, modelID string) *CartesiaProvider {
	if modelID == "" {
		modelID = "sonic-3"
	}
	return &CartesiaProvider{
		apiKey:     apiKey,
		modelID:    modelID,
		sampleRate: sampleRate,
		baseURL:    defaultCartesiaBaseURL,
		apiVersion: defaultCartesiaAPIVersion,
		dialer:     &websocket.Dialer{Proxy: http.ProxyFromEnvironment},
	}
}

func (c *CartesiaProvider) Connect(ctx context.Context) error {
	if c.apiKey == "" {
		return errors.New("cartesia api key missing")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	wsURL, err := c.buildWebsocketURL()
	if err != nil {
		return err
	}

	conn, _, err := c.dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial cartesia websocket: %w", err)
	}

	c.conn = conn
	c.active = false
	return nil
}

func (c *CartesiaProvider) Stream(ctx context.Context, req SynthesisRequest) (<-chan AudioChunk, <-chan error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, nil, errors.New("cartesia websocket not connected")
	}
	if c.active {
		return nil, nil, errors.New("cartesia stream already in progress")
	}
	c.active = true

	contextID := uuid.NewString()
	modelID := req.ModelID
	if modelID == "" {
		modelID = c.modelID
	}
	payload := map[string]any{
		"model_id":   modelID,
		"transcript": req.Text,
		"voice": map[string]string{
			"mode": "id",
			"id":   req.VoiceID,
		},
		"context_id": contextID,
		"stream":     true,
		"output_format": map[string]any{
			"container":   "raw",
			"encoding":    "pcm_f32le",
			"sample_rate": req.SampleRate,
		},
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.active = false
		return nil, nil, err
	}
	if err := c.conn.WriteJSON(payload); err != nil {
		c.active = false
		return nil, nil, fmt.Errorf("cartesia send: %w", err)
	}

	audioCh := make(chan AudioChunk, 8)
	errCh := make(chan error, 1)

	go func(conn *websocket.Conn) {
		defer close(audioCh)
		defer close(errCh)
		defer func() {
			c.mu.Lock()
			c.active = false
			c.mu.Unlock()
		}()

		for {
			if err := conn.SetReadDeadline(time.Now().Add(cartesiaReadTimeout)); err != nil {
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
				if errors.Is(err, net.ErrClosed) || websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					errCh <- io.EOF
				} else {
					errCh <- fmt.Errorf("cartesia read: %w", err)
				}
				return
			}

			var resp cartesiaResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				errCh <- fmt.Errorf("cartesia decode: %w", err)
				return
			}
			if resp.ContextID != "" && resp.ContextID != contextID {
				continue
			}

			switch resp.Type {
			case "chunk":
				audioBytes, err := base64.StdEncoding.DecodeString(resp.Data)
				if err != nil {
					errCh <- fmt.Errorf("cartesia base64: %w", err)
					return
				}
				samples, err := decodePCMFloat32(audioBytes)
				if err != nil {
					errCh <- fmt.Errorf("cartesia pcm decode: %w", err)
					return
				}
				select {
				case audioCh <- AudioChunk{Samples: samples}:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			case "done":
				return
			case "error":
				errCh <- fmt.Errorf("cartesia error: %s", resp.Error)
				return
			default:
				// ignore timestamps & flush notifications
			}
		}
	}(c.conn)

	return audioCh, errCh, nil
}

func (c *CartesiaProvider) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}
	closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	_ = c.conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(time.Second))
	err := c.conn.Close()
	c.conn = nil
	c.active = false
	return err
}

func (c *CartesiaProvider) buildWebsocketURL() (string, error) {
	base := c.baseURL
	if base == "" {
		base = defaultCartesiaBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse cartesia base url: %w", err)
	}
	u.Path = "/tts/websocket"
	query := u.Query()
	query.Set("api_key", c.apiKey)
	version := c.apiVersion
	if version == "" {
		version = defaultCartesiaAPIVersion
	}
	query.Set("cartesia_version", version)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func decodePCMFloat32(data []byte) ([]float32, error) {
	if len(data)%4 != 0 {
		return nil, fmt.Errorf("pcm length %d not aligned to float32", len(data))
	}
	count := len(data) / 4
	out := make([]float32, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4:])
		out[i] = math.Float32frombits(bits)
	}
	return out, nil
}
