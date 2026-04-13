package tts

import "strings"

var (
	cartesiaVoices = map[string]string{
		"default":            "a0e99841-438c-4a64-b679-ae501e7d6091",
		"british-lady":       "79a125e8-cd45-4c13-8a67-188112f4dd22",
		"barbershop-man":     "41534e16-2966-4c6b-9670-111411def906",
		"calm-lady":          "156fb8d2-335b-4950-9cb3-a2d33befec77",
		"friendly-guy":       "694f9389-aac1-45b6-b726-9d9369183238",
		"enthusiastic-lady":  "a249eaff-1e96-4d2d-b42b-b6b354b3d510",
		"professional-woman": "b9de4a89-2257-424b-94c2-db18ba68c81a",
		"storyteller":        "a3520a8f-226a-428d-9c9d-a5e5a1f4c2e0",
		"conversational-man": "2ee87190-8f84-4925-97da-e52547f9462c",
		"Katie":              "f786b574-daa5-4673-aa0c-cbe3e8534c02",
		"Kiefer":             "228fca29-3a0a-435c-8728-5cb483251068",
	}

	// bulbul:v2 speakers
	sarvamVoicesV2 = map[string]string{
		"default":  "anushka",
		"anushka":  "anushka",
		"abhilash": "abhilash",
		"manisha":  "manisha",
		"vidya":    "vidya",
		"arya":     "arya",
		"karun":    "karun",
		"hitesh":   "hitesh",
	}

	// bulbul:v3 speakers
	sarvamVoicesV3 = map[string]string{
		"default":  "aditya",
		"aditya":   "aditya",
		"ritu":     "ritu",
		"priya":    "priya",
		"neha":     "neha",
		"rahul":    "rahul",
		"pooja":    "pooja",
		"rohan":    "rohan",
		"simran":   "simran",
		"kavya":    "kavya",
		"amit":     "amit",
		"dev":      "dev",
		"ishita":   "ishita",
		"shreya":   "shreya",
		"ratan":    "ratan",
		"varun":    "varun",
		"manan":    "manan",
		"sumit":    "sumit",
		"roopa":    "roopa",
		"kabir":    "kabir",
		"aayan":    "aayan",
		"shubh":    "shubh",
		"ashutosh": "ashutosh",
		"advait":   "advait",
		"amelia":   "amelia",
		"sophia":   "sophia",
	}

	// sarvamVoices kept as a combined lookup for backward compatibility
	sarvamVoices = map[string]string{
		"default": "aditya",
	}

	// cartesiaTTSLanguages maps model → (language name → code)
	cartesiaTTSLanguages = map[string]map[string]string{
		"sonic-3": {
			"english":    "en",
			"french":     "fr",
			"german":     "de",
			"spanish":    "es",
			"portuguese": "pt",
			"chinese":    "zh",
			"japanese":   "ja",
			"hindi":      "hi",
			"italian":    "it",
			"korean":     "ko",
			"dutch":      "nl",
			"polish":     "pl",
			"russian":    "ru",
			"swedish":    "sv",
			"turkish":    "tr",
			"tagalog":    "tl",
			"bulgarian":  "bg",
			"romanian":   "ro",
			"arabic":     "ar",
			"czech":      "cs",
			"greek":      "el",
			"finnish":    "fi",
			"croatian":   "hr",
			"malay":      "ms",
			"slovak":     "sk",
			"danish":     "da",
			"tamil":      "ta",
			"ukrainian":  "uk",
			"hungarian":  "hu",
			"norwegian":  "no",
			"vietnamese": "vi",
			"bengali":    "bn",
			"thai":       "th",
			"hebrew":     "he",
			"georgian":   "ka",
			"indonesian": "id",
			"telugu":     "te",
			"gujarati":   "gu",
			"kannada":    "kn",
			"malayalam":  "ml",
			"marathi":    "mr",
			"punjabi":    "pa",
		},
	}

	// sarvamTTSLanguages maps model → (language name → BCP-47 code)
	sarvamTTSLanguages = map[string]map[string]string{
		"bulbul:v3": {
			"hindi":     "hi-IN",
			"bengali":   "bn-IN",
			"tamil":     "ta-IN",
			"telugu":    "te-IN",
			"gujarati":  "gu-IN",
			"kannada":   "kn-IN",
			"malayalam": "ml-IN",
			"marathi":   "mr-IN",
			"punjabi":   "pa-IN",
			"odia":      "od-IN",
			"english":   "en-IN",
		},
		"bulbul:v2": {
			"hindi":     "hi-IN",
			"bengali":   "bn-IN",
			"tamil":     "ta-IN",
			"telugu":    "te-IN",
			"gujarati":  "gu-IN",
			"kannada":   "kn-IN",
			"malayalam": "ml-IN",
			"marathi":   "mr-IN",
			"punjabi":   "pa-IN",
			"odia":      "od-IN",
			"english":   "en-IN",
		},
	}
)

// ResolveCartesiaLanguage converts friendly language names to Cartesia language codes for the given model.
func ResolveCartesiaLanguage(name string, modelID string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if langs, ok := cartesiaTTSLanguages[modelID]; ok {
		if code, ok := langs[key]; ok {
			return code
		}
	}
	return name
}

// ResolveCartesiaVoice converts friendly names to Cartesia preset IDs.
func ResolveCartesiaVoice(name string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if id, ok := cartesiaVoices[key]; ok {
		return id
	}
	return name
}

// ResolveSarvamVoice converts friendly names to Sarvam voice IDs.
// It selects from the correct speaker set based on the model version.
func ResolveSarvamVoice(name string, modelID string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	voices := sarvamVoicesV2
	if modelID == "bulbul:v3" {
		voices = sarvamVoicesV3
	}
	if id, ok := voices[key]; ok {
		return id
	}
	// Fallback: check the other model's voices
	fallback := sarvamVoicesV3
	if modelID == "bulbul:v3" {
		fallback = sarvamVoicesV2
	}
	if id, ok := fallback[key]; ok {
		return id
	}
	return name
}

// ResolveSarvamLanguage converts preset language names to Sarvam locale codes for the given model.
func ResolveSarvamLanguage(name string, modelID string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if langs, ok := sarvamTTSLanguages[modelID]; ok {
		if code, ok := langs[key]; ok {
			return code
		}
	}
	return name
}
