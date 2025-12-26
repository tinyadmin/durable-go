package memory

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/tinyadmin/durable-go/notify"
	"github.com/tinyadmin/durable-go/storage"
	"github.com/tinyadmin/durable-go/stream"
)

// message represents a single message in a stream.
type message struct {
	offset string
	data   []byte
}

// streamData holds the data for a single stream.
type streamData struct {
	mu          sync.RWMutex
	url         string
	contentType string
	createdAt   time.Time
	ttl         *time.Duration
	expiresAt   *time.Time
	messages    []message
	tailOffset  string
	lastSeq     string
	offsetGen   *stream.Generator
	isJSON      bool
}

// Storage is an in-memory implementation of storage.Storage.
type Storage struct {
	mu       sync.RWMutex
	streams  map[string]*streamData
	notifier *notify.MemoryNotifier
	closed   bool
}

// New creates a new in-memory storage.
func New() *Storage {
	return &Storage{
		streams:  make(map[string]*streamData),
		notifier: notify.NewMemory(),
	}
}

// CreateStream creates a new stream or returns existing if config matches.
func (s *Storage) CreateStream(ctx context.Context, url string, opts storage.CreateOptions) (*storage.StreamMetadata, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	contentType := opts.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Check if stream exists
	if existing, ok := s.streams[url]; ok {
		existing.mu.RLock()

		// Check if expired - treat as non-existent
		if existing.expiresAt != nil && time.Now().After(*existing.expiresAt) {
			existing.mu.RUnlock()
			// Delete the expired stream and create new one
			delete(s.streams, url)
		} else {
			defer existing.mu.RUnlock()

			// Check config matches
			if storage.NormalizeContentType(existing.contentType) != storage.NormalizeContentType(contentType) {
				return nil, false, storage.ErrStreamConfigMismatch
			}

			// Check TTL matches
			if !ttlEqual(existing.ttl, opts.TTL) {
				return nil, false, storage.ErrStreamConfigMismatch
			}

			// Check ExpiresAt matches (only if not using TTL)
			if opts.TTL == nil && !expiresAtEqual(existing.expiresAt, opts.ExpiresAt) {
				return nil, false, storage.ErrStreamConfigMismatch
			}

			// Return existing metadata
			return &storage.StreamMetadata{
				URL:         url,
				ContentType: existing.contentType,
				CreatedAt:   existing.createdAt,
				TTL:         existing.ttl,
				ExpiresAt:   existing.expiresAt,
				TailOffset:  existing.tailOffset,
				LastSeq:     existing.lastSeq,
			}, false, nil
		}
	}

	// Create new stream
	now := time.Now()
	offsetGen := stream.NewGenerator()
	initialOffset := offsetGen.Next(0)

	// Calculate expiresAt from TTL if provided
	var expiresAt *time.Time
	if opts.TTL != nil {
		exp := now.Add(*opts.TTL)
		expiresAt = &exp
	} else if opts.ExpiresAt != nil {
		expiresAt = opts.ExpiresAt
	}

	sd := &streamData{
		url:         url,
		contentType: contentType,
		createdAt:   now,
		ttl:         opts.TTL,
		expiresAt:   expiresAt,
		messages:    make([]message, 0),
		tailOffset:  initialOffset.String(),
		offsetGen:   offsetGen,
		isJSON:      storage.IsJSONContentType(contentType),
	}

	// Handle initial data if provided
	if len(opts.InitialData) > 0 {
		if sd.isJSON {
			processor := stream.NewJSONProcessor()
			messages, err := processor.ValidateAndFlatten(opts.InitialData, true) // Allow empty array on PUT
			if err != nil {
				return nil, false, err
			}
			for _, msg := range messages {
				offset := offsetGen.Next(len(msg))
				sd.messages = append(sd.messages, message{offset: offset.String(), data: msg})
				sd.tailOffset = offset.String()
			}
		} else {
			offset := offsetGen.Next(len(opts.InitialData))
			sd.messages = append(sd.messages, message{offset: offset.String(), data: opts.InitialData})
			sd.tailOffset = offset.String()
		}
	}

	s.streams[url] = sd

	return &storage.StreamMetadata{
		URL:         url,
		ContentType: contentType,
		CreatedAt:   now,
		TTL:         opts.TTL,
		ExpiresAt:   opts.ExpiresAt,
		TailOffset:  sd.tailOffset,
		LastSeq:     sd.lastSeq,
	}, true, nil
}

// DeleteStream removes a stream and all its data.
func (s *Storage) DeleteStream(ctx context.Context, url string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.streams[url]; !ok {
		return storage.ErrStreamNotFound
	}

	delete(s.streams, url)
	s.notifier.NotifyError(url, storage.ErrStreamNotFound)
	return nil
}

// GetMetadata retrieves stream metadata.
func (s *Storage) GetMetadata(ctx context.Context, url string) (*storage.StreamMetadata, error) {
	s.mu.RLock()
	sd, ok := s.streams[url]
	s.mu.RUnlock()

	if !ok {
		return nil, storage.ErrStreamNotFound
	}

	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// Check expiry
	if sd.expiresAt != nil && time.Now().After(*sd.expiresAt) {
		return nil, storage.ErrStreamNotFound
	}

	return &storage.StreamMetadata{
		URL:         url,
		ContentType: sd.contentType,
		CreatedAt:   sd.createdAt,
		TTL:         sd.ttl,
		ExpiresAt:   sd.expiresAt,
		TailOffset:  sd.tailOffset,
		LastSeq:     sd.lastSeq,
	}, nil
}

// Append adds data to the stream.
func (s *Storage) Append(ctx context.Context, url string, data []byte, opts storage.AppendOptions) (*storage.AppendResult, error) {
	if len(data) == 0 {
		return nil, storage.ErrEmptyAppend
	}

	s.mu.RLock()
	sd, ok := s.streams[url]
	s.mu.RUnlock()

	if !ok {
		return nil, storage.ErrStreamNotFound
	}

	sd.mu.Lock()
	defer sd.mu.Unlock()

	// Check if expired
	if sd.expiresAt != nil && time.Now().After(*sd.expiresAt) {
		return nil, storage.ErrStreamNotFound
	}

	// Check content type matches
	if opts.ContentType != "" {
		if storage.NormalizeContentType(opts.ContentType) != storage.NormalizeContentType(sd.contentType) {
			return nil, storage.ErrContentTypeMismatch
		}
	}

	// Check sequence number monotonicity
	if opts.SeqNumber != "" {
		if sd.lastSeq != "" && opts.SeqNumber <= sd.lastSeq {
			return nil, storage.ErrSequenceRegression
		}
		sd.lastSeq = opts.SeqNumber
	}

	var lastOffset string

	if sd.isJSON {
		processor := stream.NewJSONProcessor()
		messages, err := processor.ValidateAndFlatten(data, false) // Don't allow empty array on POST
		if err != nil {
			return nil, err
		}
		for _, msg := range messages {
			offset := sd.offsetGen.Next(len(msg))
			sd.messages = append(sd.messages, message{offset: offset.String(), data: msg})
			lastOffset = offset.String()
		}
	} else {
		offset := sd.offsetGen.Next(len(data))
		sd.messages = append(sd.messages, message{offset: offset.String(), data: data})
		lastOffset = offset.String()
	}

	sd.tailOffset = lastOffset

	// Notify subscribers
	s.notifier.Notify(url, lastOffset)

	return &storage.AppendResult{NextOffset: lastOffset}, nil
}

// Read reads data from the stream starting at offset.
func (s *Storage) Read(ctx context.Context, url string, offset string) (*storage.ReadResult, error) {
	s.mu.RLock()
	sd, ok := s.streams[url]
	s.mu.RUnlock()

	if !ok {
		return nil, storage.ErrStreamNotFound
	}

	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// Check if expired
	if sd.expiresAt != nil && time.Now().After(*sd.expiresAt) {
		return nil, storage.ErrStreamNotFound
	}

	// Parse offset
	startOffset, err := stream.Parse(offset)
	if err != nil {
		return nil, err
	}

	// Find messages after offset
	var resultMessages [][]byte
	var nextOffset string

	startOffsetStr := ""
	if !startOffset.IsZero() {
		startOffsetStr = startOffset.String()
	}

	for _, msg := range sd.messages {
		if startOffsetStr == "" || msg.offset > startOffsetStr {
			resultMessages = append(resultMessages, msg.data)
			nextOffset = msg.offset
		}
	}

	// Determine next offset
	if nextOffset == "" {
		nextOffset = sd.tailOffset
	}

	// Format response based on content type
	var responseData []byte
	if sd.isJSON {
		processor := stream.NewJSONProcessor()
		responseData = processor.FormatResponse(resultMessages)
	} else {
		// Concatenate all messages
		var buf bytes.Buffer
		for _, msg := range resultMessages {
			buf.Write(msg)
		}
		responseData = buf.Bytes()
	}

	return &storage.ReadResult{
		Data:        io.NopCloser(bytes.NewReader(responseData)),
		NextOffset:  nextOffset,
		UpToDate:    len(resultMessages) == 0 || nextOffset == sd.tailOffset,
		ContentType: sd.contentType,
	}, nil
}

// Subscribe registers for notifications when new data is available.
func (s *Storage) Subscribe(ctx context.Context, url string, offset string) (<-chan storage.Notification, error) {
	s.mu.RLock()
	sd, ok := s.streams[url]
	s.mu.RUnlock()

	if !ok {
		return nil, storage.ErrStreamNotFound
	}

	sd.mu.RLock()
	expired := sd.expiresAt != nil && time.Now().After(*sd.expiresAt)
	sd.mu.RUnlock()

	if expired {
		return nil, storage.ErrStreamNotFound
	}

	return s.notifier.Subscribe(url, offset), nil
}

// Unsubscribe removes a notification subscription.
func (s *Storage) Unsubscribe(ctx context.Context, url string, ch <-chan storage.Notification) error {
	s.notifier.Unsubscribe(url, ch)
	return nil
}

// Close releases any resources held by the storage.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.notifier.Close()
	s.streams = nil
	return nil
}

// ttlEqual compares two TTL pointers for equality.
func ttlEqual(a, b *time.Duration) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// expiresAtEqual compares two ExpiresAt pointers for equality.
func expiresAtEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}
