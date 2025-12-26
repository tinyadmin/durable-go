package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrStreamNotFound      = errors.New("stream not found")
	ErrStreamExists        = errors.New("stream already exists")
	ErrStreamConfigMismatch = errors.New("stream exists with different configuration")
	ErrContentTypeMismatch = errors.New("content type does not match stream")
	ErrSequenceRegression  = errors.New("sequence number regression")
	ErrEmptyAppend         = errors.New("empty append not allowed")
	ErrOffsetNotFound      = errors.New("offset not found or expired")
)

// StreamMetadata contains stream configuration and state.
type StreamMetadata struct {
	URL         string
	ContentType string
	CreatedAt   time.Time
	TTL         *time.Duration // nil if no TTL
	ExpiresAt   *time.Time     // nil if no expiry
	TailOffset  string         // Current end of stream (next offset)
	LastSeq     string         // Last sequence number
}

// CreateOptions for stream creation.
type CreateOptions struct {
	ContentType string
	TTL         *time.Duration
	ExpiresAt   *time.Time
	InitialData []byte // Optional initial data for PUT with body
}

// AppendOptions for append operations.
type AppendOptions struct {
	ContentType string
	SeqNumber   string // For monotonicity enforcement
}

// AppendResult contains the result of an append operation.
type AppendResult struct {
	NextOffset string
}

// ReadResult contains data from a read operation.
type ReadResult struct {
	Data       io.ReadCloser
	NextOffset string
	UpToDate   bool
	Cursor     string
	ContentType string
}

// Notification signals new data availability.
type Notification struct {
	Offset string
	Error  error
}

// Storage defines the interface for stream storage backends.
type Storage interface {
	// CreateStream creates a new stream or returns existing if config matches.
	// Returns ErrStreamConfigMismatch if stream exists with different config.
	CreateStream(ctx context.Context, url string, opts CreateOptions) (*StreamMetadata, bool, error)

	// DeleteStream removes a stream and all its data.
	// Returns ErrStreamNotFound if stream doesn't exist.
	DeleteStream(ctx context.Context, url string) error

	// GetMetadata retrieves stream metadata.
	// Returns ErrStreamNotFound if stream doesn't exist.
	GetMetadata(ctx context.Context, url string) (*StreamMetadata, error)

	// Append adds data to the stream.
	// Returns ErrStreamNotFound if stream doesn't exist.
	// Returns ErrContentTypeMismatch if content type doesn't match.
	// Returns ErrSequenceRegression if sequence number is not increasing.
	// Returns ErrEmptyAppend if data is empty.
	Append(ctx context.Context, url string, data []byte, opts AppendOptions) (*AppendResult, error)

	// Read reads data from the stream starting at offset.
	// Returns ErrStreamNotFound if stream doesn't exist.
	// Returns ErrOffsetNotFound if offset is invalid or before retention.
	Read(ctx context.Context, url string, offset string) (*ReadResult, error)

	// Subscribe registers for notifications when new data is available.
	// The returned channel will receive notifications until Unsubscribe is called.
	Subscribe(ctx context.Context, url string, offset string) (<-chan Notification, error)

	// Unsubscribe removes a notification subscription.
	Unsubscribe(ctx context.Context, url string, ch <-chan Notification) error

	// Close releases any resources held by the storage.
	Close() error
}

// IsJSONContentType returns true if the content type is JSON.
func IsJSONContentType(contentType string) bool {
	// Normalize: extract MIME type before semicolon and lowercase
	if idx := indexOf(contentType, ';'); idx != -1 {
		contentType = contentType[:idx]
	}
	contentType = toLower(trimSpace(contentType))
	return contentType == "application/json"
}

// NormalizeContentType extracts the MIME type without parameters, lowercased.
func NormalizeContentType(contentType string) string {
	if idx := indexOf(contentType, ';'); idx != -1 {
		contentType = contentType[:idx]
	}
	return toLower(trimSpace(contentType))
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
