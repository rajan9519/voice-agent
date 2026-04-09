package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"voiceagent/internal/agent"
	"voiceagent/internal/llm"
	"voiceagent/internal/stt"
	"voiceagent/internal/tts"
)

// voiceAgent is the common interface implemented by both agent types.
type voiceAgent interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

const idleTimeout = 2 * time.Minute

// Session manages a single WebSocket voice session.
type Session struct {
	conn *websocket.Conn

	// sendMu serialises all writes to the WebSocket connection.
	sendMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	// audioFeed pushes client audio into the STT recorder.
	audioFeed chan<- stt.AudioChunk

	agent    voiceAgent
	recorder *WebSocketRecorder

	// lastActivity tracks the last time meaningful conversational activity
	// occurred (transcript or reply). Stored as unix nanoseconds.
	lastActivity atomic.Int64
}

// HandleConnection runs the full lifecycle of a WebSocket session.
// It blocks until the connection is closed or the parent context is cancelled.
func HandleConnection(parent context.Context, conn *websocket.Conn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	s := &Session{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
	}

	defer s.cleanup()
	go s.idleWatcher()
	s.readPump()
}

// readPump processes incoming WebSocket messages.
func (s *Session) readPump() {
	for {
		msgType, data, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				s.logf("read error: %v", err)
			}
			return
		}

		select {
		case <-s.ctx.Done():
			return
		default:
		}

		switch msgType {
		case websocket.BinaryMessage:
			s.handleAudio(data)
		case websocket.TextMessage:
			s.handleJSON(data)
		}
	}
}

// handleJSON dispatches JSON control messages from the client.
func (s *Session) handleJSON(data []byte) {
	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.sendError("invalid JSON: " + err.Error())
		return
	}

	switch msg.Type {
	case TypeSessionStart:
		var start SessionStartMessage
		if err := json.Unmarshal(data, &start); err != nil {
			s.sendError("invalid session.start: " + err.Error())
			return
		}
		s.handleSessionStart(start.Config)

	case TypeSessionEnd:
		s.cancel()

	default:
		s.sendError(fmt.Sprintf("unknown message type: %q", msg.Type))
	}
}

// handleAudio forwards raw PCM bytes from the client into the STT pipeline.
func (s *Session) handleAudio(data []byte) {
	if s.audioFeed == nil {
		return
	}
	// data is raw 16kHz mono int16 LE PCM from the client.
	chunk := stt.AudioChunk{}
	// We copy data so the websocket buffer can be reused.
	copied := make([]byte, len(data))
	copy(copied, data)
	chunk = stt.NewAudioChunk(copied)

	select {
	case s.audioFeed <- chunk:
	case <-s.ctx.Done():
	}
}

// handleSessionStart configures and launches the voice agent pipeline.
func (s *Session) handleSessionStart(cfg SessionConfig) {
	if s.agent != nil {
		s.sendError("session already started")
		return
	}

	// Apply defaults.
	if cfg.Mode == "" {
		cfg.Mode = "conversation"
	}
	if cfg.SourceLanguage == "" {
		cfg.SourceLanguage = "english"
	}
	if cfg.TTSProvider == "" {
		cfg.TTSProvider = "cartesia"
	}
	if cfg.TTSVoice == "" {
		cfg.TTSVoice = "default"
	}
	if cfg.TTSSampleRate <= 0 {
		cfg.TTSSampleRate = 22050
	}
	if cfg.FlushThreshold <= 0 {
		cfg.FlushThreshold = 120
	}

	// --- STT provider ---
	sttCfg := stt.ProviderConfig{
		CartesiaKey: os.Getenv("CARTESIA_API_KEY"),
		SarvamKey:   os.Getenv("SARVAM_API_KEY"),
		HighVAD:     true,
		VADSignals:  true,
	}

	var sttProv stt.Provider
	var langCode string
	var err error

	if cfg.STTProvider != "" {
		sttProv, langCode, err = stt.SelectProviderExplicit(cfg.STTProvider, cfg.SourceLanguage, sttCfg)
	} else {
		sttProv, langCode, err = stt.SelectProvider(cfg.SourceLanguage, sttCfg)
	}
	if err != nil {
		s.sendError("stt provider: " + err.Error())
		return
	}

	// --- WebSocket recorder ---
	recorder, feed := NewWebSocketRecorder()
	s.recorder = recorder
	s.audioFeed = feed

	sttPipeline, err := stt.New(sttProv, recorder, stt.Options{Language: langCode})
	if err != nil {
		s.sendError("stt pipeline: " + err.Error())
		return
	}

	// In translation mode, TTS must speak the target language.
	// If the caller did not explicitly set tts_language, derive it from target_language.
	if strings.ToLower(strings.TrimSpace(cfg.Mode)) == "translation" && cfg.TTSLanguage == "" {
		cfg.TTSLanguage = cfg.TargetLanguage
	}

	// --- TTS engine ---
	ttsEngine, err := s.configureTTS(cfg)
	if err != nil {
		s.sendError("tts engine: " + err.Error())
		return
	}

	// --- Audio player (sends audio back over WebSocket) ---
	player := NewWebSocketPlayer(s.sendBinary)

	// --- LLM client ---
	llmClient, err := llm.New(llm.Config{
		DefaultModel: strings.TrimSpace(cfg.Model),
	})
	if err != nil {
		s.sendError("llm client: " + err.Error())
		return
	}

	// --- Build agent ---
	var va voiceAgent

	switch strings.ToLower(strings.TrimSpace(cfg.Mode)) {
	case "translation":
		va, err = agent.NewTranslationAgent(sttPipeline, ttsEngine, llmClient, player, agent.TranslationOptions{
			TargetLanguage:        strings.TrimSpace(cfg.TargetLanguage),
			TranslationModel:      strings.TrimSpace(cfg.Model),
			FlushThreshold:        cfg.FlushThreshold,
			UsePartialTranscripts: cfg.UsePartials,
		})
	default:
		va, err = agent.NewConversationAgent(sttPipeline, ttsEngine, llmClient, player, agent.ConversationOptions{
			ConversationModel:       strings.TrimSpace(cfg.Model),
			FlushThreshold:          cfg.FlushThreshold,
			TranscriptQueueCapacity: 5,
			UsePartialTranscripts:   cfg.UsePartials,
			EventCallback:           s.onAgentEvent,
		})
	}
	if err != nil {
		s.sendError("agent: " + err.Error())
		return
	}

	if err := va.Start(s.ctx); err != nil {
		s.sendError("start agent: " + err.Error())
		return
	}

	s.agent = va
	s.lastActivity.Store(time.Now().UnixNano())
	s.sendJSON(ServerEvent{Type: TypeSessionStarted})
	s.logf("session started mode=%s lang=%s", cfg.Mode, cfg.SourceLanguage)
}

// idleWatcher closes the session if there is no meaningful conversational
// activity (transcript or reply) for idleTimeout.
func (s *Session) idleWatcher() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			last := s.lastActivity.Load()
			if last == 0 {
				// Session hasn't started yet; don't count idle time.
				continue
			}
			if time.Since(time.Unix(0, last)) > idleTimeout {
				s.logf("idle timeout: no meaningful activity for %v, closing connection", idleTimeout)
				s.sendJSON(ServerEvent{
					Type:    TypeSessionClosed,
					Message: "connection closed due to inactivity",
				})
				s.cancel()
				return
			}
		}
	}
}

// onAgentEvent is the EventCallback wired into the conversation agent.
// It forwards transcript and reply events to the client.
func (s *Session) onAgentEvent(eventType string, text string, isFinal bool) {
	switch eventType {
	case "transcript":
		s.lastActivity.Store(time.Now().UnixNano())
		s.sendJSON(ServerEvent{Type: TypeTranscript, Text: text, IsFinal: isFinal})
	case "reply":
		s.lastActivity.Store(time.Now().UnixNano())
		s.sendJSON(ServerEvent{Type: TypeReply, Text: text, IsFinal: isFinal})
	}
}

// configureTTS builds a TTS engine from the session config.
func (s *Session) configureTTS(cfg SessionConfig) (*tts.TextToSpeech, error) {
	name := strings.ToLower(strings.TrimSpace(cfg.TTSProvider))

	switch name {
	case "cartesia":
		apiKey := strings.TrimSpace(os.Getenv("CARTESIA_API_KEY"))
		if apiKey == "" {
			return nil, errors.New("CARTESIA_API_KEY not set")
		}
		voiceID := tts.ResolveCartesiaVoice(cfg.TTSVoice)
		provider := tts.NewCartesiaProvider(apiKey, cfg.TTSSampleRate, cfg.TTSModel)
		return tts.New(provider, tts.Options{
			SampleRate: cfg.TTSSampleRate,
			VoiceID:    voiceID,
			ModelID:    cfg.TTSModel,
		})

	case "sarvam":
		apiKey := strings.TrimSpace(os.Getenv("SARVAM_API_KEY"))
		if apiKey == "" {
			return nil, errors.New("SARVAM_API_KEY not set")
		}
		voiceID := tts.ResolveSarvamVoice(cfg.TTSVoice, cfg.TTSModel)
		lang := cfg.TTSLanguage
		if lang == "" {
			lang = cfg.SourceLanguage
		}
		languageCode := tts.ResolveSarvamLanguage(lang)
		provider := tts.NewSarvamProvider(apiKey, cfg.TTSSampleRate, languageCode, cfg.TTSModel)
		return tts.New(provider, tts.Options{
			SampleRate: cfg.TTSSampleRate,
			VoiceID:    voiceID,
			ModelID:    cfg.TTSModel,
			Language:   languageCode,
		})

	default:
		return nil, fmt.Errorf("unsupported TTS provider %q", name)
	}
}

// sendJSON serialises an event and sends it as a text WebSocket message.
func (s *Session) sendJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		s.logf("marshal error: %v", err)
		return
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		s.logf("write error: %v", err)
	}
}

// sendBinary sends raw bytes as a binary WebSocket message.
// It is safe for concurrent use.
func (s *Session) sendBinary(data []byte) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.conn.WriteMessage(websocket.BinaryMessage, data)
}

// sendError sends an error event to the client.
func (s *Session) sendError(msg string) {
	s.logf("error: %s", msg)
	s.sendJSON(ServerEvent{Type: TypeError, Message: msg})
}

// cleanup tears down the agent and closes the connection.
func (s *Session) cleanup() {
	s.cancel()

	if s.agent != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.agent.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.logf("agent stop error: %v", err)
		}
	}

	if s.recorder != nil {
		_ = s.recorder.Close()
	}

	_ = s.conn.Close()
	s.logf("session closed")
}

func (s *Session) logf(format string, args ...any) {
	log.Printf("[ws_session] "+format, args...)
}
