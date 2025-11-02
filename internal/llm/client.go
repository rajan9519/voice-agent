package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const (
	defaultBaseURL = "https://api.groq.com/openai/v1"
	defaultModel   = "llama-3.1-8b-instant"
)

// Config defines how the LLM client connects to the Groq/OpenAI compatible API.
type Config struct {
	APIKey       string
	BaseURL      string
	DefaultModel string
	HTTPClient   *http.Client
}

// Client issues streaming Responses API calls.
type Client struct {
	apiKey string
	model  string
	url    string

	httpClient *http.Client
}

// New constructs a streaming LLM client using environment-aware defaults.
func New(cfg Config) (*Client, error) {
	apiKey := strings.TrimSpace(cfg.APIKey)
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("GROQ_API_KEY"))
	}
	if apiKey == "" {
		return nil, errors.New("llm: GROQ_API_KEY must be set")
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
	}
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	model := strings.TrimSpace(cfg.DefaultModel)
	if model == "" {
		model = strings.TrimSpace(os.Getenv("OPENAI_TRANSLATION_MODEL"))
	}
	if model == "" {
		model = defaultModel
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 0,
		}
	}

	return &Client{
		apiKey:     apiKey,
		model:      model,
		url:        baseURL + "/responses",
		httpClient: client,
	}, nil
}

// MessageContent represents an individual content block inside a message.
type MessageContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// Message matches the Responses API message schema (role + content blocks).
type Message struct {
	Role    string           `json:"role"`
	Content []MessageContent `json:"content"`
}

// Request defines the payload needed to start a streaming LLM call.
type Request struct {
	Model    string
	Messages []Message
}

// StreamResponse exposes model deltas and async errors.
type StreamResponse struct {
	Text <-chan string
	Err  <-chan error
}

// Stream issues a streaming Responses API call and returns channels for the emitted text deltas.
func (c *Client) Stream(ctx context.Context, req Request) (*StreamResponse, error) {
	if c == nil {
		return nil, errors.New("llm: client is nil")
	}
	if len(req.Messages) == 0 {
		return nil, errors.New("llm: at least one message is required")
	}

	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = c.model
	}

	payload := responsesRequest{
		Model:  model,
		Input:  req.Messages,
		Stream: true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("llm: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("llm: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("llm: request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("llm: unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	textCh := make(chan string)
	errCh := make(chan error, 1)

	go func() {
		defer close(textCh)
		defer close(errCh)
		defer resp.Body.Close()

		reader := bufio.NewReader(resp.Body)
		for {
			event, data, readErr := readSSEEvent(reader)
			if readErr != nil {
				if errors.Is(readErr, io.EOF) || errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
					return
				}
				select {
				case errCh <- fmt.Errorf("llm: stream read: %w", readErr):
				default:
				}
				return
			}

			switch event {
			case "":
				continue
			case "response.output_text.delta", "response.refusal.delta":
				var payload responseDelta
				if err := json.Unmarshal(data, &payload); err != nil {
					select {
					case errCh <- fmt.Errorf("llm: decode delta: %w", err):
					default:
					}
					return
				}
				if payload.Delta != "" {
					select {
					case textCh <- payload.Delta:
					case <-ctx.Done():
						return
					}
				}
			case "response.error":
				var payload responseError
				if err := json.Unmarshal(data, &payload); err != nil {
					select {
					case errCh <- fmt.Errorf("llm: decode error event: %w", err):
					default:
					}
					return
				}
				msg := strings.TrimSpace(payload.Error.Message)
				if msg == "" {
					msg = "llm request failed"
				}
				select {
				case errCh <- errors.New(msg):
				default:
				}
				return
			case "response.completed":
				return
			default:
				continue
			}
		}
	}()

	return &StreamResponse{
		Text: textCh,
		Err:  errCh,
	}, nil
}

// TranslateOptions describe how to build a translation-specific prompt.
type TranslateOptions struct {
	TargetLanguage string
	Model          string
}

const defaultLanguage = "English"

// StreamTranslate is a convenience helper that reuses the general Stream method for translation use cases.
func (c *Client) StreamTranslate(ctx context.Context, text string, opts TranslateOptions) (*StreamResponse, error) {
	src := strings.TrimSpace(text)
	if src == "" {
		return nil, errors.New("translate: source text must not be empty")
	}

	target := strings.TrimSpace(opts.TargetLanguage)
	if target == "" {
		target = defaultLanguage
	}

	req := Request{
		Model: opts.Model,
		Messages: []Message{
			{
				Role: "system",
				Content: []MessageContent{
					{
						Type: "input_text",
						Text: fmt.Sprintf(
							"You are a dedicated translation engine. Translate the user's message into the requested language (%s) while preserving meaning and formatting. Respond with the translation only, without additional commentary.",
							target,
						),
					},
				},
			},
			{
				Role: "user",
				Content: []MessageContent{
					{
						Type: "input_text",
						Text: fmt.Sprintf("Translate the following text into %s.\n\n%s", target, src),
					},
				},
			},
		},
	}

	return c.Stream(ctx, req)
}

type responsesRequest struct {
	Model  string    `json:"model"`
	Input  []Message `json:"input"`
	Stream bool      `json:"stream"`
}

type responseDelta struct {
	Type  string `json:"type"`
	Delta string `json:"delta"`
}

type responseError struct {
	Type  string              `json:"type"`
	Error responseErrorDetail `json:"error"`
}

type responseErrorDetail struct {
	Message string `json:"message"`
}

func readSSEEvent(r *bufio.Reader) (string, []byte, error) {
	var (
		event string
		data  []byte
	)

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) == 0 {
				return "", nil, io.EOF
			}
			if len(line) == 0 {
				return "", nil, err
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(line[6:])
			} else if strings.HasPrefix(line, "data:") {
				data = appendDataLine(data, line[5:])
			}
			continue
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event == "" && len(data) == 0 {
				continue
			}
			break
		}

		if strings.HasPrefix(line, ":") {
			continue
		}

		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[6:])
		case strings.HasPrefix(line, "data:"):
			data = appendDataLine(data, line[5:])
		}
	}

	if len(data) == 0 {
		return event, nil, nil
	}

	return event, data, nil
}

func appendDataLine(dst []byte, line string) []byte {
	line = strings.TrimLeft(line, " ")
	if len(dst) > 0 {
		dst = append(dst, '\n')
	}
	return append(dst, []byte(line)...)
}
