package llm

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// readSSEData reads one SSE event from r and returns the concatenated `data:` payload.
// Lines beginning with `:` are comments and ignored. An empty line terminates the event.
// Returns io.EOF when the stream is fully consumed.
func readSSEData(r *bufio.Reader) ([]byte, error) {
	var data []byte
	sawAny := false

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(line) == 0 && !sawAny {
					return nil, io.EOF
				}
				line = strings.TrimRight(line, "\r\n")
				if line != "" && !strings.HasPrefix(line, ":") {
					if strings.HasPrefix(line, "data:") {
						data = appendDataLine(data, line[5:])
					}
				}
				return data, nil
			}
			return nil, err
		}

		sawAny = true
		line = strings.TrimRight(line, "\r\n")

		if line == "" {
			if len(data) == 0 {
				sawAny = false
				continue
			}
			return data, nil
		}

		if strings.HasPrefix(line, ":") {
			continue
		}

		if strings.HasPrefix(line, "data:") {
			data = appendDataLine(data, line[5:])
		}
	}
}

// readSSEEvent reads one SSE event including its `event:` name and data payload.
// An empty line terminates the event. Returns io.EOF when the stream is fully consumed.
func readSSEEvent(r *bufio.Reader) (string, []byte, error) {
	var (
		event string
		data  []byte
	)

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) == 0 {
				return "", nil, io.EOF
			}
			if len(line) == 0 {
				return "", nil, err
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(line[6:])
			} else if strings.HasPrefix(line, "data:") {
				data = appendDataLine(data, line[5:])
			}
			continue
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event == "" && len(data) == 0 {
				continue
			}
			break
		}

		if strings.HasPrefix(line, ":") {
			continue
		}

		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[6:])
		case strings.HasPrefix(line, "data:"):
			data = appendDataLine(data, line[5:])
		}
	}

	if len(data) == 0 {
		return event, nil, nil
	}

	return event, data, nil
}

func appendDataLine(dst []byte, line string) []byte {
	line = strings.TrimLeft(line, " ")
	if len(dst) > 0 {
		dst = append(dst, '\n')
	}
	return append(dst, []byte(line)...)
}
