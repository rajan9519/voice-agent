package llm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// APIType selects which OpenAI-compatible surface the client speaks to.
type APIType string

const (
	APIChatCompletions APIType = "chat_completions"
	APIResponses       APIType = "responses"
)

const (
	defaultBaseURL = "https://api.cerebras.ai/v1"
	defaultModel   = "gpt-oss-120b"
	defaultAPIType = APIChatCompletions
)

// Config defines how the LLM client connects to the upstream OpenAI-compatible API.
type Config struct {
	APIKey       string
	BaseURL      string
	DefaultModel string
	APIType      APIType
	HTTPClient   *http.Client
}

// Client is the user-facing entry point. It delegates the actual HTTP/SSE work
// to a backend that implements either the Chat Completions or Responses API.
type Client struct {
	model   string
	backend backend
}

// backend is the swappable transport for a specific API surface.
type backend interface {
	stream(ctx context.Context, model string, messages []Message) (*StreamResponse, error)
}

// New constructs a streaming LLM client using environment-aware defaults.
// The API surface (chat_completions vs responses) can be picked via Config.APIType
// or the LLM_API_TYPE environment variable.
func New(cfg Config) (*Client, error) {
	apiKey := strings.TrimSpace(cfg.APIKey)
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("CEREBRAS_API_KEY"))
	}
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("GROQ_API_KEY"))
	}
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	}
	if apiKey == "" {
		return nil, errors.New("llm: api key must be set (CEREBRAS_API_KEY, GROQ_API_KEY, or OPENAI_API_KEY)")
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

	apiType := cfg.APIType
	if apiType == "" {
		apiType = APIType(strings.TrimSpace(os.Getenv("LLM_API_TYPE")))
	}
	if apiType == "" {
		apiType = defaultAPIType
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 0}
	}

	var be backend
	switch apiType {
	case APIChatCompletions:
		be = &chatCompletionsBackend{
			apiKey:     apiKey,
			url:        baseURL + "/chat/completions",
			httpClient: httpClient,
		}
	case APIResponses:
		be = &responsesBackend{
			apiKey:     apiKey,
			url:        baseURL + "/responses",
			httpClient: httpClient,
		}
	default:
		return nil, fmt.Errorf("llm: unsupported APIType %q", apiType)
	}

	return &Client{model: model, backend: be}, nil
}

// MessageContent represents an individual content block inside a message.
type MessageContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// Message is the API-agnostic message shape used by callers; each backend
// adapts it to the wire format it needs.
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

// Stream issues a streaming request via the configured backend.
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

	return c.backend.stream(ctx, model, req.Messages)
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

// ConversationOptions describe how to build a conversation-specific prompt.
type ConversationOptions struct {
	Model   string
	History []Message
}

// StreamConverse is a convenience helper that issues a single conversational turn.
// It replies in the same language the user spoke, keeping answers short and natural.
func (c *Client) StreamConverse(ctx context.Context, text string, opts ConversationOptions) (*StreamResponse, error) {
	src := strings.TrimSpace(text)
	if src == "" {
		return nil, errors.New("converse: input text must not be empty")
	}

	systemMsg := Message{
		Role: "system",
		Content: []MessageContent{
			{
				Type: "input_text",
				Text: "You are a friendly conversational assistant. Always reply in the same language the user is speaking. Keep your answers short and natural — the way a person actually talks, usually just one or two sentences. No bullet points, no long explanations, just a casual human reply.",
			},
		},
	}

	messages := make([]Message, 0, 1+len(opts.History)+1)
	messages = append(messages, systemMsg)
	messages = append(messages, opts.History...)
	messages = append(messages, Message{
		Role: "user",
		Content: []MessageContent{
			{Type: "input_text", Text: src},
		},
	})

	return c.Stream(ctx, Request{Model: opts.Model, Messages: messages})
}

// flattenContent collapses content blocks down to a single string, used by
// backends whose wire format expects plain string content for text-only messages.
func flattenContent(parts []MessageContent) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0].Text
	}
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(p.Text)
	}
	return b.String()
}

type responseErrorDetail struct {
	Message string `json:"message"`
}
