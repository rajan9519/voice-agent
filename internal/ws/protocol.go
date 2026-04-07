package ws

// Client → Server message types.
const (
	TypeSessionStart = "session.start"
	TypeSessionEnd   = "session.end"
)

// Server → Client message types.
const (
	TypeSessionStarted = "session.started"
	TypeTranscript     = "transcript"
	TypeReply          = "reply"
	TypeError          = "error"
)

// SessionStartMessage is sent by the client to configure and begin a session.
type SessionStartMessage struct {
	Type   string        `json:"type"`
	Config SessionConfig `json:"config"`
}

// SessionConfig carries the parameters for a new voice session.
type SessionConfig struct {
	Mode           string `json:"mode"`             // "conversation" or "translation"
	SourceLanguage string `json:"source_language"`   // e.g. "english", "hindi"
	TargetLanguage string `json:"target_language"`   // translation mode only
	STTProvider    string `json:"stt_provider"`      // "cartesia" or "sarvam" (optional, auto-detect)
	TTSProvider    string `json:"tts_provider"`      // "cartesia" or "sarvam"
	TTSVoice       string `json:"tts_voice"`         // voice preset name or ID
	TTSLanguage    string `json:"tts_language"`      // TTS language preset (Sarvam only)
	TTSModel       string `json:"tts_model"`         // override TTS model
	TTSSampleRate  int    `json:"tts_sample_rate"`   // TTS output sample rate (default 22050)
	Model          string `json:"model"`             // LLM model override
	FlushThreshold int    `json:"flush_threshold"`   // chars before forcing TTS flush
	UsePartials    bool   `json:"use_partials"`      // forward partial STT transcripts
}

// ClientMessage is the envelope used to peek at the "type" field of incoming JSON.
type ClientMessage struct {
	Type string `json:"type"`
}

// ServerEvent is a JSON message sent from server to client.
type ServerEvent struct {
	Type    string `json:"type"`
	Text    string `json:"text,omitempty"`
	IsFinal bool   `json:"is_final,omitempty"`
	Message string `json:"message,omitempty"`
}
