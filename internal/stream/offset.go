package stream

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidOffset = errors.New("invalid offset format")
)

// Offset represents a position in the stream.
// Format: {timestamp_hex}-{sequence_hex}-{byte_position_hex}
// Example: "01945e2a3b4c-0001-00000400"
type Offset struct {
	Timestamp    int64  // Milliseconds since epoch
	Sequence     uint32 // Sequence within same millisecond
	BytePosition uint64 // Byte position in the logical stream
}

// Zero returns the zero offset (beginning of stream).
func Zero() Offset {
	return Offset{}
}

// String returns the lexicographically sortable string representation.
func (o Offset) String() string {
	return fmt.Sprintf("%012x-%04x-%016x", o.Timestamp, o.Sequence, o.BytePosition)
}

// IsZero returns true if this is the beginning offset.
func (o Offset) IsZero() bool {
	return o.Timestamp == 0 && o.Sequence == 0 && o.BytePosition == 0
}

// Compare returns -1, 0, or 1 for lexicographic comparison.
func (o Offset) Compare(other Offset) int {
	return strings.Compare(o.String(), other.String())
}

// After returns true if o comes after other in the stream.
func (o Offset) After(other Offset) bool {
	return o.Compare(other) > 0
}

// Parse parses an offset string. Returns zero offset for "-1" or empty string.
func Parse(s string) (Offset, error) {
	if s == "-1" || s == "" {
		return Zero(), nil
	}

	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return Offset{}, ErrInvalidOffset
	}

	ts, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return Offset{}, ErrInvalidOffset
	}

	seq, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return Offset{}, ErrInvalidOffset
	}

	pos, err := strconv.ParseUint(parts[2], 16, 64)
	if err != nil {
		return Offset{}, ErrInvalidOffset
	}

	return Offset{
		Timestamp:    ts,
		Sequence:     uint32(seq),
		BytePosition: pos,
	}, nil
}

// Generator generates monotonically increasing offsets.
type Generator struct {
	mu           sync.Mutex
	lastTime     int64
	lastSequence uint32
	bytePosition uint64
}

// NewGenerator creates a new offset generator.
func NewGenerator() *Generator {
	return &Generator{}
}

// Next generates the next offset for data of the given size.
func (g *Generator) Next(dataSize int) Offset {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()

	if now == g.lastTime {
		g.lastSequence++
	} else {
		g.lastTime = now
		g.lastSequence = 0
	}

	offset := Offset{
		Timestamp:    g.lastTime,
		Sequence:     g.lastSequence,
		BytePosition: g.bytePosition,
	}

	g.bytePosition += uint64(dataSize)

	return offset
}

// Current returns the current tail offset (next position to write).
func (g *Generator) Current() Offset {
	g.mu.Lock()
	defer g.mu.Unlock()

	return Offset{
		Timestamp:    g.lastTime,
		Sequence:     g.lastSequence,
		BytePosition: g.bytePosition,
	}
}

// SetPosition sets the generator's position (used when restoring state).
func (g *Generator) SetPosition(offset Offset) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.lastTime = offset.Timestamp
	g.lastSequence = offset.Sequence
	g.bytePosition = offset.BytePosition
}
