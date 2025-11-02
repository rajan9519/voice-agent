package stt

import (
	"errors"
	"fmt"
	"strings"
)

// ProviderConfig gathers optional provider-specific settings sourced from the environment.
type ProviderConfig struct {
	CartesiaKey string
	SarvamKey   string
	SarvamModel string
	HighVAD     bool
	VADSignals  bool
}

var (
	sarvamLanguages = map[string]string{
		"hindi":      "hi-IN",
		"kannada":    "kn-IN",
		"tamil":      "ta-IN",
		"telugu":     "te-IN",
		"malayalam":  "ml-IN",
		"marathi":    "mr-IN",
		"bengali":    "bn-IN",
		"gujarati":   "gu-IN",
		"punjabi":    "pa-IN",
		"odia":       "od-IN",
		"english-in": "en-IN",
	}

	cartesiaLanguages = map[string]string{
		"english":    "en",
		"spanish":    "es",
		"french":     "fr",
		"german":     "de",
		"italian":    "it",
		"portuguese": "pt",
		"dutch":      "nl",
		"russian":    "ru",
		"chinese":    "zh",
		"japanese":   "ja",
		"korean":     "ko",
	}
)

// SelectProvider resolves the best matching streaming provider for the requested language.
func SelectProvider(language string, cfg ProviderConfig) (Provider, string, error) {
	lang := strings.ToLower(strings.TrimSpace(language))
	if lang == "" {
		return nil, "", errors.New("stt: language is required")
	}

	if code, ok := sarvamLanguages[lang]; ok {
		if cfg.SarvamKey == "" {
			return nil, "", errors.New("stt: SARVAM_API_KEY missing")
		}
		provider := NewSarvamProvider(cfg.SarvamKey, SarvamOptions{
			Model:              cfg.SarvamModel,
			HighVADSensitivity: cfg.HighVAD,
			VADSignals:         cfg.VADSignals,
		})
		return provider, code, nil
	}

	if code, ok := cartesiaLanguages[lang]; ok {
		if cfg.CartesiaKey == "" {
			return nil, "", errors.New("stt: CARTESIA_API_KEY missing")
		}
		provider := NewCartesiaProvider(cfg.CartesiaKey, CartesiaOptions{})
		return provider, code, nil
	}

	return nil, "", fmt.Errorf("stt: unsupported language %q", language)
}
