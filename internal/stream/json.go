package stream

import (
	"bytes"
	"encoding/json"
	"errors"
)

var (
	ErrInvalidJSON = errors.New("invalid JSON")
	ErrEmptyArray  = errors.New("empty array not allowed in append")
)

// JSONProcessor handles JSON mode message parsing.
type JSONProcessor struct{}

// NewJSONProcessor creates a new JSON processor.
func NewJSONProcessor() *JSONProcessor {
	return &JSONProcessor{}
}

// ValidateAndFlatten validates JSON and flattens top-level arrays.
// Returns individual messages to store.
// For arrays: each element becomes a separate message (one level of flattening).
// For objects/values: stored as single message.
func (p *JSONProcessor) ValidateAndFlatten(data []byte, allowEmptyArray bool) ([][]byte, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, ErrInvalidJSON
	}

	// Check if it's an array
	if data[0] == '[' {
		return p.flattenArray(data, allowEmptyArray)
	}

	// Single value - validate JSON
	if !json.Valid(data) {
		return nil, ErrInvalidJSON
	}

	// Compact to normalize whitespace
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return nil, ErrInvalidJSON
	}

	return [][]byte{buf.Bytes()}, nil
}

func (p *JSONProcessor) flattenArray(data []byte, allowEmptyArray bool) ([][]byte, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil, ErrInvalidJSON
	}

	if len(arr) == 0 {
		if allowEmptyArray {
			return [][]byte{}, nil
		}
		return nil, ErrEmptyArray
	}

	messages := make([][]byte, len(arr))
	for i, msg := range arr {
		// Compact each message to normalize
		var buf bytes.Buffer
		if err := json.Compact(&buf, msg); err != nil {
			return nil, ErrInvalidJSON
		}
		messages[i] = buf.Bytes()
	}

	return messages, nil
}

// FormatResponse formats messages as a JSON array for GET response.
func (p *JSONProcessor) FormatResponse(messages [][]byte) []byte {
	if len(messages) == 0 {
		return []byte("[]")
	}

	var buf bytes.Buffer
	buf.WriteByte('[')

	for i, msg := range messages {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(msg)
	}

	buf.WriteByte(']')
	return buf.Bytes()
}
