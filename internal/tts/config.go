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
	}

	sarvamVoices = map[string]string{
		"default": "anushka",
		"anushka": "anushka",
		"meera":   "meera",
		"arvind":  "arvind",
		"raghav":  "raghav",
	}

	sarvamLanguages = map[string]string{
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
	}
)

// ResolveCartesiaVoice converts friendly names to Cartesia preset IDs.
func ResolveCartesiaVoice(name string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if id, ok := cartesiaVoices[key]; ok {
		return id
	}
	return name
}

// ResolveSarvamVoice converts friendly names to Sarvam voice IDs.
func ResolveSarvamVoice(name string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if id, ok := sarvamVoices[key]; ok {
		return id
	}
	return name
}

// ResolveSarvamLanguage converts preset language names to Sarvam locale codes.
func ResolveSarvamLanguage(name string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if code, ok := sarvamLanguages[key]; ok {
		return code
	}
	return name
}
