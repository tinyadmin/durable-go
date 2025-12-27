package rediss3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tinyadmin/durable-go/storage"
	"github.com/tinyadmin/durable-go/stream"
)

// Redis key prefixes
const (
	keyMeta = "stream:%s:meta"
	keyMsgs = "stream:%s:msgs"
	keyTail = "stream:%s:tail"
	keyGen  = "stream:%s:gen"
)

// Pub/sub channel pattern
const channelPattern = "stream:%s:notify"

// Storage implements storage.Storage using Redis for hot data and optional S3 for archives.
type Storage struct {
	rdb  *redis.Client
	opts Options

	// In-memory caches
	generators sync.Map // url -> *stream.Generator

	// Subscriber management
	subsMu sync.RWMutex
	subs   map[string]map[chan storage.Notification]struct{} // url -> set of channels

	// Lifecycle
	closeCh chan struct{}
	wg      sync.WaitGroup
	closed  bool
	closeMu sync.Mutex
}

// New creates a new Redis+S3 storage backend.
func New(opts Options) (*Storage, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     opts.RedisAddr,
		Password: opts.RedisPassword,
		DB:       opts.RedisDB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	s := &Storage{
		rdb:     rdb,
		opts:    opts,
		subs:    make(map[string]map[chan storage.Notification]struct{}),
		closeCh: make(chan struct{}),
	}

	// Start background workers
	s.wg.Add(1)
	go s.runSubscriber()

	if opts.CleanupInterval > 0 {
		s.wg.Add(1)
		go s.runCleanup()
	}

	return s, nil
}

// metaFields represents the stream metadata stored in Redis hash.
type metaFields struct {
	ContentType        string `json:"content_type"`
	CreatedAt          int64  `json:"created_at"`
	TTLMs              int64  `json:"ttl_ms,omitempty"`
	ExpiresAt          int64  `json:"expires_at,omitempty"`
	IsJSON             bool   `json:"is_json"`
	LastSeq            string `json:"last_seq,omitempty"`
	OffsetLastTime     int64  `json:"offset_last_time"`
	OffsetLastSequence uint32 `json:"offset_last_sequence"`
	OffsetBytePosition uint64 `json:"offset_byte_position"`
}

func metaKey(url string) string   { return fmt.Sprintf(keyMeta, url) }
func msgsKey(url string) string   { return fmt.Sprintf(keyMsgs, url) }
func tailKey(url string) string   { return fmt.Sprintf(keyTail, url) }
func genKey(url string) string    { return fmt.Sprintf(keyGen, url) }
func channel(url string) string   { return fmt.Sprintf(channelPattern, url) }

// CreateStream creates a new stream or returns existing if config matches.
func (s *Storage) CreateStream(ctx context.Context, url string, opts storage.CreateOptions) (*storage.StreamMetadata, bool, error) {
	contentType := opts.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	now := time.Now()
	nowMs := now.UnixMilli()

	// Check if stream exists
	existing, err := s.getMetaFields(ctx, url)
	if err == nil {
		// Stream exists - check if expired
		if existing.ExpiresAt > 0 && nowMs > existing.ExpiresAt {
			// Expired - delete and recreate
			if err := s.deleteStreamKeys(ctx, url); err != nil {
				return nil, false, err
			}
		} else {
			// Not expired - validate config matches
			if storage.NormalizeContentType(existing.ContentType) != storage.NormalizeContentType(contentType) {
				return nil, false, storage.ErrStreamConfigMismatch
			}
			// Check TTL match
			var existingTTL *time.Duration
			if existing.TTLMs > 0 {
				d := time.Duration(existing.TTLMs) * time.Millisecond
				existingTTL = &d
			}
			if !ttlEqual(existingTTL, opts.TTL) {
				return nil, false, storage.ErrStreamConfigMismatch
			}

			// Return existing metadata
			meta := s.fieldsToMetadata(url, existing)
			return meta, false, nil
		}
	}

	// Create new stream
	offsetGen := stream.NewGenerator()
	initialOffset := offsetGen.Next(0)

	var expiresAtMs int64
	var ttlMs int64
	var expiresAt *time.Time

	if opts.TTL != nil {
		ttlMs = opts.TTL.Milliseconds()
		exp := now.Add(*opts.TTL)
		expiresAt = &exp
		expiresAtMs = exp.UnixMilli()
	} else if opts.ExpiresAt != nil {
		expiresAt = opts.ExpiresAt
		expiresAtMs = opts.ExpiresAt.UnixMilli()
	}

	isJSON := storage.IsJSONContentType(contentType)
	tailOffset := initialOffset.String()

	// Store metadata
	meta := metaFields{
		ContentType:        contentType,
		CreatedAt:          nowMs,
		TTLMs:              ttlMs,
		ExpiresAt:          expiresAtMs,
		IsJSON:             isJSON,
		OffsetLastTime:     initialOffset.Timestamp,
		OffsetLastSequence: initialOffset.Sequence,
		OffsetBytePosition: initialOffset.BytePosition,
	}

	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, metaKey(url), map[string]interface{}{
		"content_type":         meta.ContentType,
		"created_at":           meta.CreatedAt,
		"ttl_ms":               meta.TTLMs,
		"expires_at":           meta.ExpiresAt,
		"is_json":              boolToInt(meta.IsJSON),
		"last_seq":             "",
		"offset_last_time":     meta.OffsetLastTime,
		"offset_last_sequence": meta.OffsetLastSequence,
		"offset_byte_position": meta.OffsetBytePosition,
	})
	pipe.Set(ctx, tailKey(url), tailOffset, 0)

	// Handle initial data
	if len(opts.InitialData) > 0 {
		var messages [][]byte
		if isJSON {
			processor := stream.NewJSONProcessor()
			messages, err = processor.ValidateAndFlatten(opts.InitialData, true)
			if err != nil {
				return nil, false, fmt.Errorf("create stream %s: %w", url, err)
			}
		} else {
			messages = [][]byte{opts.InitialData}
		}

		for _, msg := range messages {
			offset := offsetGen.Next(len(msg))
			// Store as "offset:data" for lexicographic sorting
			member := offset.String() + ":" + string(msg)
			pipe.ZAdd(ctx, msgsKey(url), redis.Z{Score: 0, Member: member})
			tailOffset = offset.String()
		}

		// Update tail and generator state
		current := offsetGen.Current()
		pipe.Set(ctx, tailKey(url), tailOffset, 0)
		pipe.HSet(ctx, metaKey(url), map[string]interface{}{
			"offset_last_time":     current.Timestamp,
			"offset_last_sequence": current.Sequence,
			"offset_byte_position": current.BytePosition,
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, false, err
	}

	// Cache generator
	s.generators.Store(url, offsetGen)

	return &storage.StreamMetadata{
		URL:         url,
		ContentType: contentType,
		CreatedAt:   now,
		TTL:         opts.TTL,
		ExpiresAt:   expiresAt,
		TailOffset:  tailOffset,
		LastSeq:     "",
	}, true, nil
}

// DeleteStream removes a stream and all its data.
func (s *Storage) DeleteStream(ctx context.Context, url string) error {
	// Check if exists first
	exists, err := s.rdb.Exists(ctx, metaKey(url)).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return storage.ErrStreamNotFound
	}

	if err := s.deleteStreamKeys(ctx, url); err != nil {
		return err
	}

	// Notify subscribers of deletion
	s.notifyError(url, storage.ErrStreamNotFound)

	return nil
}

// GetMetadata retrieves stream metadata.
func (s *Storage) GetMetadata(ctx context.Context, url string) (*storage.StreamMetadata, error) {
	fields, err := s.getMetaFields(ctx, url)
	if err != nil {
		return nil, storage.ErrStreamNotFound
	}

	// Check expiry
	if fields.ExpiresAt > 0 && time.Now().UnixMilli() > fields.ExpiresAt {
		return nil, storage.ErrStreamNotFound
	}

	// Get current tail
	tail, err := s.rdb.Get(ctx, tailKey(url)).Result()
	if err != nil {
		return nil, err
	}

	meta := s.fieldsToMetadata(url, fields)
	meta.TailOffset = tail
	return meta, nil
}

// Append adds data to the stream.
func (s *Storage) Append(ctx context.Context, url string, data []byte, opts storage.AppendOptions) (*storage.AppendResult, error) {
	if len(data) == 0 {
		return nil, storage.ErrEmptyAppend
	}

	// Get metadata
	fields, err := s.getMetaFields(ctx, url)
	if err != nil {
		return nil, storage.ErrStreamNotFound
	}

	// Check expiry
	if fields.ExpiresAt > 0 && time.Now().UnixMilli() > fields.ExpiresAt {
		return nil, storage.ErrStreamNotFound
	}

	// Validate content type
	if opts.ContentType != "" {
		if storage.NormalizeContentType(opts.ContentType) != storage.NormalizeContentType(fields.ContentType) {
			return nil, storage.ErrContentTypeMismatch
		}
	}

	// Check sequence monotonicity
	if opts.SeqNumber != "" && fields.LastSeq != "" {
		if opts.SeqNumber <= fields.LastSeq {
			return nil, storage.ErrSequenceRegression
		}
	}

	// Get or restore offset generator
	gen := s.getOrCreateGenerator(url, fields)

	// Parse messages
	var messages [][]byte
	if fields.IsJSON {
		processor := stream.NewJSONProcessor()
		messages, err = processor.ValidateAndFlatten(data, false)
		if err != nil {
			return nil, fmt.Errorf("append to stream %s: %w", url, err)
		}
	} else {
		messages = [][]byte{data}
	}

	// Add messages to Redis
	pipe := s.rdb.Pipeline()
	var lastOffset string

	for _, msg := range messages {
		offset := gen.Next(len(msg))
		member := offset.String() + ":" + string(msg)
		pipe.ZAdd(ctx, msgsKey(url), redis.Z{Score: 0, Member: member})
		lastOffset = offset.String()
	}

	// Update tail and generator state
	current := gen.Current()
	pipe.Set(ctx, tailKey(url), lastOffset, 0)

	updateFields := map[string]interface{}{
		"offset_last_time":     current.Timestamp,
		"offset_last_sequence": current.Sequence,
		"offset_byte_position": current.BytePosition,
	}
	if opts.SeqNumber != "" {
		updateFields["last_seq"] = opts.SeqNumber
	}
	pipe.HSet(ctx, metaKey(url), updateFields)

	// Publish notification
	pipe.Publish(ctx, channel(url), lastOffset)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	return &storage.AppendResult{NextOffset: lastOffset}, nil
}

// Read reads data from the stream starting at offset.
func (s *Storage) Read(ctx context.Context, url string, offset string) (*storage.ReadResult, error) {
	// Get metadata
	fields, err := s.getMetaFields(ctx, url)
	if err != nil {
		return nil, storage.ErrStreamNotFound
	}

	// Check expiry
	if fields.ExpiresAt > 0 && time.Now().UnixMilli() > fields.ExpiresAt {
		return nil, storage.ErrStreamNotFound
	}

	// Parse start offset
	startOffset, err := stream.Parse(offset)
	if err != nil {
		return nil, fmt.Errorf("read stream %s: invalid offset %q: %w", url, offset, err)
	}

	// Get current tail
	tail, err := s.rdb.Get(ctx, tailKey(url)).Result()
	if err != nil {
		return nil, err
	}

	// Read messages using ZRANGEBYLEX
	var min string
	if startOffset.IsZero() {
		min = "-"
	} else {
		min = "(" + startOffset.String() // Exclusive
	}

	members, err := s.rdb.ZRangeByLex(ctx, msgsKey(url), &redis.ZRangeBy{
		Min: min,
		Max: "+",
	}).Result()
	if err != nil {
		return nil, err
	}

	// Parse messages
	var resultMessages [][]byte
	var nextOffset string

	for _, member := range members {
		// Split "offset:data"
		idx := strings.Index(member, ":")
		if idx == -1 {
			continue
		}
		msgOffset := member[:idx]
		msgData := []byte(member[idx+1:])
		resultMessages = append(resultMessages, msgData)
		nextOffset = msgOffset
	}

	if nextOffset == "" {
		nextOffset = tail
	}

	// Format response
	var responseData []byte
	if fields.IsJSON {
		processor := stream.NewJSONProcessor()
		responseData = processor.FormatResponse(resultMessages)
	} else {
		var buf bytes.Buffer
		for _, msg := range resultMessages {
			buf.Write(msg)
		}
		responseData = buf.Bytes()
	}

	return &storage.ReadResult{
		Data:        io.NopCloser(bytes.NewReader(responseData)),
		NextOffset:  nextOffset,
		UpToDate:    len(resultMessages) == 0 || nextOffset == tail,
		ContentType: fields.ContentType,
	}, nil
}

// Subscribe registers for notifications when new data is available.
func (s *Storage) Subscribe(ctx context.Context, url string, offset string) (<-chan storage.Notification, error) {
	// Verify stream exists
	exists, err := s.rdb.Exists(ctx, metaKey(url)).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, storage.ErrStreamNotFound
	}

	ch := make(chan storage.Notification, 1)

	s.subsMu.Lock()
	if s.subs[url] == nil {
		s.subs[url] = make(map[chan storage.Notification]struct{})
	}
	s.subs[url][ch] = struct{}{}
	s.subsMu.Unlock()

	return ch, nil
}

// Unsubscribe removes a notification subscription.
func (s *Storage) Unsubscribe(ctx context.Context, url string, ch <-chan storage.Notification) error {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	if subs, ok := s.subs[url]; ok {
		for subCh := range subs {
			if subCh == ch {
				delete(subs, subCh)
				close(subCh)
				break
			}
		}
		if len(subs) == 0 {
			delete(s.subs, url)
		}
	}

	return nil
}

// Close releases any resources held by the storage.
func (s *Storage) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	close(s.closeCh)
	s.wg.Wait()

	// Close all subscriber channels
	s.subsMu.Lock()
	for _, subs := range s.subs {
		for ch := range subs {
			close(ch)
		}
	}
	s.subs = nil
	s.subsMu.Unlock()

	return s.rdb.Close()
}

// Helper methods

func (s *Storage) getMetaFields(ctx context.Context, url string) (*metaFields, error) {
	result, err := s.rdb.HGetAll(ctx, metaKey(url)).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, storage.ErrStreamNotFound
	}

	fields := &metaFields{
		ContentType: result["content_type"],
		LastSeq:     result["last_seq"],
	}

	if v, err := strconv.ParseInt(result["created_at"], 10, 64); err == nil {
		fields.CreatedAt = v
	}
	if v, err := strconv.ParseInt(result["ttl_ms"], 10, 64); err == nil {
		fields.TTLMs = v
	}
	if v, err := strconv.ParseInt(result["expires_at"], 10, 64); err == nil {
		fields.ExpiresAt = v
	}
	if v, err := strconv.ParseInt(result["is_json"], 10, 64); err == nil {
		fields.IsJSON = v == 1
	}
	if v, err := strconv.ParseInt(result["offset_last_time"], 10, 64); err == nil {
		fields.OffsetLastTime = v
	}
	if v, err := strconv.ParseUint(result["offset_last_sequence"], 10, 32); err == nil {
		fields.OffsetLastSequence = uint32(v)
	}
	if v, err := strconv.ParseUint(result["offset_byte_position"], 10, 64); err == nil {
		fields.OffsetBytePosition = v
	}

	return fields, nil
}

func (s *Storage) fieldsToMetadata(url string, fields *metaFields) *storage.StreamMetadata {
	meta := &storage.StreamMetadata{
		URL:         url,
		ContentType: fields.ContentType,
		CreatedAt:   time.UnixMilli(fields.CreatedAt),
		LastSeq:     fields.LastSeq,
	}

	if fields.TTLMs > 0 {
		d := time.Duration(fields.TTLMs) * time.Millisecond
		meta.TTL = &d
	}
	if fields.ExpiresAt > 0 {
		t := time.UnixMilli(fields.ExpiresAt)
		meta.ExpiresAt = &t
	}

	return meta
}

func (s *Storage) getOrCreateGenerator(url string, fields *metaFields) *stream.Generator {
	if v, ok := s.generators.Load(url); ok {
		return v.(*stream.Generator)
	}

	gen := stream.NewGenerator()
	gen.SetPosition(stream.Offset{
		Timestamp:    fields.OffsetLastTime,
		Sequence:     fields.OffsetLastSequence,
		BytePosition: fields.OffsetBytePosition,
	})

	s.generators.Store(url, gen)
	return gen
}

func (s *Storage) deleteStreamKeys(ctx context.Context, url string) error {
	keys := []string{metaKey(url), msgsKey(url), tailKey(url), genKey(url)}
	return s.rdb.Del(ctx, keys...).Err()
}

func (s *Storage) notifyError(url string, err error) {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()

	if subs, ok := s.subs[url]; ok {
		for ch := range subs {
			select {
			case ch <- storage.Notification{Error: err}:
			default:
			}
		}
	}
}

// runSubscriber listens for Redis pub/sub messages and dispatches to local subscribers.
func (s *Storage) runSubscriber() {
	defer s.wg.Done()

	pubsub := s.rdb.PSubscribe(context.Background(), "stream:*:notify")
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		select {
		case <-s.closeCh:
			return
		case msg := <-ch:
			if msg == nil {
				continue
			}
			// Extract URL from channel name: "stream:{url}:notify"
			parts := strings.SplitN(msg.Channel, ":", 3)
			if len(parts) < 3 {
				continue
			}
			url := parts[1]
			offset := msg.Payload

			s.subsMu.RLock()
			if subs, ok := s.subs[url]; ok {
				for ch := range subs {
					select {
					case ch <- storage.Notification{Offset: offset}:
					default:
					}
				}
			}
			s.subsMu.RUnlock()
		}
	}
}

// runCleanup periodically removes expired streams.
func (s *Storage) runCleanup() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.opts.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

func (s *Storage) cleanupExpired() {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	// Scan for all stream metadata keys
	var cursor uint64
	for {
		keys, nextCursor, err := s.rdb.Scan(ctx, cursor, "stream:*:meta", 100).Result()
		if err != nil {
			return
		}

		for _, key := range keys {
			// Extract URL from key
			parts := strings.SplitN(key, ":", 3)
			if len(parts) < 3 {
				continue
			}
			url := parts[1]

			// Check if expired
			expiresAt, err := s.rdb.HGet(ctx, key, "expires_at").Int64()
			if err != nil || expiresAt == 0 {
				continue
			}

			if nowMs > expiresAt {
				s.deleteStreamKeys(ctx, url)
				s.notifyError(url, storage.ErrStreamNotFound)
				s.generators.Delete(url)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func ttlEqual(a, b *time.Duration) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

var _ json.Marshaler = (*metaFields)(nil)

func (m *metaFields) MarshalJSON() ([]byte, error) {
	type alias metaFields
	return json.Marshal((*alias)(m))
}
