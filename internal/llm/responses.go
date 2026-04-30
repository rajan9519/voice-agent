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
	"strings"
)

// responsesBackend speaks the OpenAI Responses API (POST /responses), which
// streams typed SSE events such as `response.output_text.delta` and
// `response.completed`.
type responsesBackend struct {
	apiKey     string
	url        string
	httpClient *http.Client
}

func (b *responsesBackend) stream(ctx context.Context, model string, msgs []Message) (*StreamResponse, error) {
	payload := responsesRequest{
		Model:  model,
		Input:  msgs,
		Stream: true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("llm: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("llm: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+b.apiKey)

	resp, err := b.httpClient.Do(httpReq)
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
				var p responseDelta
				if err := json.Unmarshal(data, &p); err != nil {
					select {
					case errCh <- fmt.Errorf("llm: decode delta: %w", err):
					default:
					}
					return
				}
				if p.Delta != "" {
					select {
					case textCh <- p.Delta:
					case <-ctx.Done():
						return
					}
				}
			case "response.error":
				var p responseError
				if err := json.Unmarshal(data, &p); err != nil {
					select {
					case errCh <- fmt.Errorf("llm: decode error event: %w", err):
					default:
					}
					return
				}
				msg := strings.TrimSpace(p.Error.Message)
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

	return &StreamResponse{Text: textCh, Err: errCh}, nil
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
