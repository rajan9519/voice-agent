package stt

import "errors"

var (
	errCartesiaNotConnected = errors.New("cartesia websocket not connected")
	errSarvamNotConnected   = errors.New("sarvam websocket not connected")
)
