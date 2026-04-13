package stt

import (
	"errors"
	"fmt"
	"strings"
)

// ProviderConfig gathers optional provider-specific settings sourced from the environment.
type ProviderConfig struct {
	CartesiaKey   string
	CartesiaModel string
	SarvamKey     string
	SarvamModel   string
	HighVAD       bool
	VADSignals    bool
}

var (
	// sarvamSTTLanguages maps model → (language name → BCP-47 code)
	sarvamSTTLanguages = map[string]map[string]string{
		"saaras:v3": {
			"hindi":     "hi-IN",
			"bengali":   "bn-IN",
			"kannada":   "kn-IN",
			"malayalam": "ml-IN",
			"marathi":   "mr-IN",
			"odia":      "od-IN",
			"punjabi":   "pa-IN",
			"tamil":     "ta-IN",
			"telugu":    "te-IN",
			"gujarati":  "gu-IN",
			"english":   "en-IN",
			"assamese":  "as-IN",
			"urdu":      "ur-IN",
			"nepali":    "ne-IN",
			"konkani":   "kok-IN",
			"kashmiri":  "ks-IN",
			"sindhi":    "sd-IN",
			"sanskrit":  "sa-IN",
			"santali":   "sat-IN",
			"manipuri":  "mni-IN",
			"bodo":      "brx-IN",
			"maithili":  "mai-IN",
			"dogri":     "doi-IN",
		},
	}

	// cartesiaSTTLanguages maps model → (language name → code)
	cartesiaSTTLanguages = map[string]map[string]string{
		"ink-whisper": {
			"english":         "en",
			"chinese":         "zh",
			"german":          "de",
			"spanish":         "es",
			"russian":         "ru",
			"korean":          "ko",
			"french":          "fr",
			"japanese":        "ja",
			"portuguese":      "pt",
			"turkish":         "tr",
			"polish":          "pl",
			"catalan":         "ca",
			"dutch":           "nl",
			"arabic":          "ar",
			"swedish":         "sv",
			"italian":         "it",
			"indonesian":      "id",
			"hindi":           "hi",
			"finnish":         "fi",
			"vietnamese":      "vi",
			"hebrew":          "he",
			"ukrainian":       "uk",
			"greek":           "el",
			"malay":           "ms",
			"czech":           "cs",
			"romanian":        "ro",
			"danish":          "da",
			"hungarian":       "hu",
			"tamil":           "ta",
			"norwegian":       "no",
			"thai":            "th",
			"urdu":            "ur",
			"croatian":        "hr",
			"bulgarian":       "bg",
			"lithuanian":      "lt",
			"latin":           "la",
			"maori":           "mi",
			"malayalam":       "ml",
			"welsh":           "cy",
			"slovak":          "sk",
			"telugu":          "te",
			"persian":         "fa",
			"latvian":         "lv",
			"bengali":         "bn",
			"serbian":         "sr",
			"azerbaijani":     "az",
			"slovenian":       "sl",
			"kannada":         "kn",
			"estonian":        "et",
			"macedonian":      "mk",
			"breton":          "br",
			"basque":          "eu",
			"icelandic":       "is",
			"armenian":        "hy",
			"nepali":          "ne",
			"mongolian":       "mn",
			"bosnian":         "bs",
			"kazakh":          "kk",
			"albanian":        "sq",
			"swahili":         "sw",
			"galician":        "gl",
			"marathi":         "mr",
			"punjabi":         "pa",
			"sinhala":         "si",
			"khmer":           "km",
			"shona":           "sn",
			"yoruba":          "yo",
			"somali":          "so",
			"afrikaans":       "af",
			"occitan":         "oc",
			"georgian":        "ka",
			"belarusian":      "be",
			"tajik":           "tg",
			"sindhi":          "sd",
			"gujarati":        "gu",
			"amharic":         "am",
			"yiddish":         "yi",
			"lao":             "lo",
			"uzbek":           "uz",
			"faroese":         "fo",
			"haitian creole":  "ht",
			"pashto":          "ps",
			"turkmen":         "tk",
			"nynorsk":         "nn",
			"maltese":         "mt",
			"sanskrit":        "sa",
			"luxembourgish":   "lb",
			"burmese":         "my",
			"tibetan":         "bo",
			"tagalog":         "tl",
			"malagasy":        "mg",
			"assamese":        "as",
			"tatar":           "tt",
			"hawaiian":        "haw",
			"lingala":         "ln",
			"hausa":           "ha",
			"bashkir":         "ba",
			"javanese":        "jw",
			"sundanese":       "su",
			"cantonese":       "yue",
		},
	}
)

func resolveSTTLanguage(langs map[string]map[string]string, model, name string) (string, bool) {
	if model == "" {
		for _, m := range langs {
			if code, ok := m[name]; ok {
				return code, true
			}
		}
		return "", false
	}
	if m, ok := langs[model]; ok {
		code, ok := m[name]
		return code, ok
	}
	return "", false
}

// SelectProvider resolves the best matching streaming provider for the requested language.
func SelectProvider(language string, cfg ProviderConfig) (Provider, string, error) {
	lang := strings.ToLower(strings.TrimSpace(language))
	if lang == "" {
		return nil, "", errors.New("stt: language is required")
	}

	sarvamModel := cfg.SarvamModel
	if sarvamModel == "" {
		sarvamModel = "saaras:v3"
	}
	if code, ok := resolveSTTLanguage(sarvamSTTLanguages, sarvamModel, lang); ok {
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

	cartesiaModel := cfg.CartesiaModel
	if cartesiaModel == "" {
		cartesiaModel = "ink-whisper"
	}
	if code, ok := resolveSTTLanguage(cartesiaSTTLanguages, cartesiaModel, lang); ok {
		if cfg.CartesiaKey == "" {
			return nil, "", errors.New("stt: CARTESIA_API_KEY missing")
		}
		provider := NewCartesiaProvider(cfg.CartesiaKey, CartesiaOptions{Model: cfg.CartesiaModel})
		return provider, code, nil
	}

	return nil, "", fmt.Errorf("stt: unsupported language %q", language)
}

// SelectProviderExplicit creates a provider for the explicitly specified provider name and language.
func SelectProviderExplicit(providerName, language string, cfg ProviderConfig) (Provider, string, error) {
	lang := strings.ToLower(strings.TrimSpace(language))
	if lang == "" {
		return nil, "", errors.New("stt: language is required")
	}

	provider := strings.ToLower(strings.TrimSpace(providerName))

	switch provider {
	case "sarvam":
		model := cfg.SarvamModel
		if model == "" {
			model = "saaras:v3"
		}
		code, ok := resolveSTTLanguage(sarvamSTTLanguages, model, lang)
		if !ok {
			return nil, "", fmt.Errorf("stt: language %q not supported by Sarvam provider", language)
		}
		if cfg.SarvamKey == "" {
			return nil, "", errors.New("stt: SARVAM_API_KEY missing")
		}
		prov := NewSarvamProvider(cfg.SarvamKey, SarvamOptions{
			Model:              cfg.SarvamModel,
			HighVADSensitivity: cfg.HighVAD,
			VADSignals:         cfg.VADSignals,
		})
		return prov, code, nil

	case "cartesia":
		model := cfg.CartesiaModel
		if model == "" {
			model = "ink-whisper"
		}
		code, ok := resolveSTTLanguage(cartesiaSTTLanguages, model, lang)
		if !ok {
			return nil, "", fmt.Errorf("stt: language %q not supported by Cartesia provider", language)
		}
		if cfg.CartesiaKey == "" {
			return nil, "", errors.New("stt: CARTESIA_API_KEY missing")
		}
		prov := NewCartesiaProvider(cfg.CartesiaKey, CartesiaOptions{Model: cfg.CartesiaModel})
		return prov, code, nil

	default:
		return nil, "", fmt.Errorf("stt: unsupported provider %q (supported: cartesia, sarvam)", providerName)
	}
}
