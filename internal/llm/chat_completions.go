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

// chatCompletionsBackend speaks the OpenAI Chat Completions API
// (POST /chat/completions, streamed via `data: {...}` SSE chunks
// terminated by `data: [DONE]`).
type chatCompletionsBackend struct {
	apiKey     string
	url        string
	httpClient *http.Client
}

func (b *chatCompletionsBackend) stream(ctx context.Context, model string, msgs []Message) (*StreamResponse, error) {
	chatMessages := make([]chatMessage, 0, len(msgs))
	for _, m := range msgs {
		chatMessages = append(chatMessages, chatMessage{
			Role:    m.Role,
			Content: flattenContent(m.Content),
		})
	}

	payload := chatCompletionRequest{
		Model:           model,
		Messages:        chatMessages,
		Stream:          true,
		ReasoningEffort: "medium",
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
			data, readErr := readSSEData(reader)
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

			if len(data) == 0 {
				continue
			}
			if string(data) == "[DONE]" {
				return
			}

			var chunk chatCompletionChunk
			if err := json.Unmarshal(data, &chunk); err != nil {
				select {
				case errCh <- fmt.Errorf("llm: decode chunk: %w", err):
				default:
				}
				return
			}

			if chunk.Error != nil {
				msg := strings.TrimSpace(chunk.Error.Message)
				if msg == "" {
					msg = "llm request failed"
				}
				select {
				case errCh <- errors.New(msg):
				default:
				}
				return
			}

			for _, choice := range chunk.Choices {
				if choice.Delta.Content == "" {
					continue
				}
				select {
				case textCh <- choice.Delta.Content:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return &StreamResponse{Text: textCh, Err: errCh}, nil
}

type chatCompletionRequest struct {
	Model           string        `json:"model"`
	Messages        []chatMessage `json:"messages"`
	Stream          bool          `json:"stream"`
	ReasoningEffort string        `json:"reasoning_effort,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionChunk struct {
	Choices []chatChoice         `json:"choices"`
	Error   *responseErrorDetail `json:"error,omitempty"`
}

type chatChoice struct {
	Delta        chatDelta `json:"delta"`
	FinishReason string    `json:"finish_reason"`
	Index        int       `json:"index"`
}

type chatDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}
