package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/tinyadmin/durable-go/notify"
	"github.com/tinyadmin/durable-go/storage"
	"github.com/tinyadmin/durable-go/stream"
	_ "modernc.org/sqlite"
)

// Options configures the SQLite storage.
type Options struct {
	Path            string
	CleanupInterval time.Duration
}

// DefaultOptions returns default configuration.
func DefaultOptions() Options {
	return Options{
		Path:            "durable.db",
		CleanupInterval: 1 * time.Minute,
	}
}

// Storage implements storage.Storage using SQLite.
type Storage struct {
	db       *sql.DB
	notifier *notify.MemoryNotifier
	opts     Options

	writeMu sync.Mutex

	generators   map[string]*stream.Generator
	generatorsMu sync.RWMutex

	cleanupDone chan struct{}
	closed      bool
	closeMu     sync.Mutex
}

// New creates a new SQLite storage.
func New(opts Options) (*Storage, error) {
	db, err := sql.Open("sqlite", opts.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := initSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	s := &Storage{
		db:          db,
		notifier:    notify.NewMemory(),
		opts:        opts,
		generators:  make(map[string]*stream.Generator),
		cleanupDone: make(chan struct{}),
	}

	go s.cleanupLoop()

	return s, nil
}

// CreateStream creates a new stream or returns existing if config matches.
func (s *Storage) CreateStream(ctx context.Context, url string, opts storage.CreateOptions) (*storage.StreamMetadata, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	contentType := opts.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	now := time.Now()
	nowMs := now.UnixMilli()

	existing, err := s.getStreamRow(ctx, url)
	if err == nil {
		if existing.expiresAt != nil && now.After(*existing.expiresAt) {
			if _, err := s.db.ExecContext(ctx, "DELETE FROM streams WHERE url = ?", url); err != nil {
				return nil, false, err
			}
			s.removeGenerator(url)
		} else {
			if storage.NormalizeContentType(existing.contentType) != storage.NormalizeContentType(contentType) {
				return nil, false, storage.ErrStreamConfigMismatch
			}
			if !ttlEqual(existing.ttl, opts.TTL) {
				return nil, false, storage.ErrStreamConfigMismatch
			}
			if opts.TTL == nil && !expiresAtEqual(existing.expiresAt, opts.ExpiresAt) {
				return nil, false, storage.ErrStreamConfigMismatch
			}

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
	} else if err != sql.ErrNoRows {
		return nil, false, err
	}

	offsetGen := stream.NewGenerator()
	initialOffset := offsetGen.Next(0)

	var expiresAtMs *int64
	var ttlMs *int64
	var expiresAt *time.Time

	if opts.TTL != nil {
		ttlMsVal := opts.TTL.Milliseconds()
		ttlMs = &ttlMsVal
		exp := now.Add(*opts.TTL)
		expiresAt = &exp
		expMs := exp.UnixMilli()
		expiresAtMs = &expMs
	} else if opts.ExpiresAt != nil {
		expiresAt = opts.ExpiresAt
		expMs := opts.ExpiresAt.UnixMilli()
		expiresAtMs = &expMs
	}

	isJSON := storage.IsJSONContentType(contentType)
	tailOffset := initialOffset.String()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	isJSONInt := 0
	if isJSON {
		isJSONInt = 1
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO streams (
			url, content_type, created_at, ttl_ms, expires_at,
			tail_offset, is_json,
			offset_last_time, offset_last_sequence, offset_byte_position
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		url, contentType, nowMs, ttlMs, expiresAtMs,
		tailOffset, isJSONInt,
		initialOffset.Timestamp, initialOffset.Sequence, initialOffset.BytePosition,
	)
	if err != nil {
		return nil, false, err
	}

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
			_, err = tx.ExecContext(ctx,
				"INSERT INTO messages (stream_url, offset, data, created_at) VALUES (?, ?, ?, ?)",
				url, offset.String(), msg, nowMs,
			)
			if err != nil {
				return nil, false, err
			}
			tailOffset = offset.String()
		}

		current := offsetGen.Current()
		_, err = tx.ExecContext(ctx, `
			UPDATE streams SET
				tail_offset = ?,
				offset_last_time = ?,
				offset_last_sequence = ?,
				offset_byte_position = ?
			WHERE url = ?`,
			tailOffset, current.Timestamp, current.Sequence, current.BytePosition, url,
		)
		if err != nil {
			return nil, false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, false, err
	}

	s.setGenerator(url, offsetGen)

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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	result, err := s.db.ExecContext(ctx, "DELETE FROM streams WHERE url = ?", url)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return storage.ErrStreamNotFound
	}

	s.removeGenerator(url)
	s.notifier.NotifyError(url, storage.ErrStreamNotFound)

	return nil
}

// GetMetadata retrieves stream metadata.
func (s *Storage) GetMetadata(ctx context.Context, url string) (*storage.StreamMetadata, error) {
	row, err := s.getStreamRow(ctx, url)
	if err == sql.ErrNoRows {
		return nil, storage.ErrStreamNotFound
	} else if err != nil {
		return nil, err
	}

	if row.expiresAt != nil && time.Now().After(*row.expiresAt) {
		return nil, storage.ErrStreamNotFound
	}

	return &storage.StreamMetadata{
		URL:         url,
		ContentType: row.contentType,
		CreatedAt:   row.createdAt,
		TTL:         row.ttl,
		ExpiresAt:   row.expiresAt,
		TailOffset:  row.tailOffset,
		LastSeq:     row.lastSeq,
	}, nil
}

// Append adds data to the stream.
func (s *Storage) Append(ctx context.Context, url string, data []byte, opts storage.AppendOptions) (*storage.AppendResult, error) {
	if len(data) == 0 {
		return nil, storage.ErrEmptyAppend
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	row, err := s.getStreamRow(ctx, url)
	if err == sql.ErrNoRows {
		return nil, storage.ErrStreamNotFound
	} else if err != nil {
		return nil, err
	}

	if row.expiresAt != nil && time.Now().After(*row.expiresAt) {
		return nil, storage.ErrStreamNotFound
	}

	if opts.ContentType != "" {
		if storage.NormalizeContentType(opts.ContentType) != storage.NormalizeContentType(row.contentType) {
			return nil, storage.ErrContentTypeMismatch
		}
	}

	if opts.SeqNumber != "" {
		if row.lastSeq != "" && opts.SeqNumber <= row.lastSeq {
			return nil, storage.ErrSequenceRegression
		}
	}

	offsetGen := s.getOrCreateGenerator(url, row)

	now := time.Now().UnixMilli()
	var lastOffset string
	var messages [][]byte

	if row.isJSON {
		processor := stream.NewJSONProcessor()
		messages, err = processor.ValidateAndFlatten(data, false)
		if err != nil {
			return nil, fmt.Errorf("append to stream %s: %w", url, err)
		}
	} else {
		messages = [][]byte{data}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, msg := range messages {
		offset := offsetGen.Next(len(msg))
		_, err = tx.ExecContext(ctx,
			"INSERT INTO messages (stream_url, offset, data, created_at) VALUES (?, ?, ?, ?)",
			url, offset.String(), msg, now,
		)
		if err != nil {
			return nil, err
		}
		lastOffset = offset.String()
	}

	current := offsetGen.Current()
	var lastSeq *string
	if opts.SeqNumber != "" {
		lastSeq = &opts.SeqNumber
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE streams SET
			tail_offset = ?,
			last_seq = COALESCE(?, last_seq),
			offset_last_time = ?,
			offset_last_sequence = ?,
			offset_byte_position = ?
		WHERE url = ?`,
		lastOffset, lastSeq, current.Timestamp, current.Sequence, current.BytePosition, url,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	s.notifier.Notify(url, lastOffset)

	return &storage.AppendResult{NextOffset: lastOffset}, nil
}

// Read reads data from the stream starting at offset.
func (s *Storage) Read(ctx context.Context, url string, offset string) (*storage.ReadResult, error) {
	row, err := s.getStreamRow(ctx, url)
	if err == sql.ErrNoRows {
		return nil, storage.ErrStreamNotFound
	} else if err != nil {
		return nil, err
	}

	if row.expiresAt != nil && time.Now().After(*row.expiresAt) {
		return nil, storage.ErrStreamNotFound
	}

	startOffset, err := stream.Parse(offset)
	if err != nil {
		return nil, fmt.Errorf("read stream %s: invalid offset %q: %w", url, offset, err)
	}

	var rows *sql.Rows
	startOffsetStr := ""
	if !startOffset.IsZero() {
		startOffsetStr = startOffset.String()
	}

	if startOffsetStr == "" {
		rows, err = s.db.QueryContext(ctx,
			"SELECT offset, data FROM messages WHERE stream_url = ? ORDER BY offset",
			url,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			"SELECT offset, data FROM messages WHERE stream_url = ? AND offset > ? ORDER BY offset",
			url, startOffsetStr,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resultMessages [][]byte
	var nextOffset string

	for rows.Next() {
		var msgOffset string
		var data []byte
		if err := rows.Scan(&msgOffset, &data); err != nil {
			return nil, err
		}
		resultMessages = append(resultMessages, data)
		nextOffset = msgOffset
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if nextOffset == "" {
		nextOffset = row.tailOffset
	}

	var responseData []byte
	if row.isJSON {
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
		UpToDate:    len(resultMessages) == 0 || nextOffset == row.tailOffset,
		ContentType: row.contentType,
	}, nil
}

// Subscribe registers for notifications when new data is available.
func (s *Storage) Subscribe(ctx context.Context, url string, offset string) (<-chan storage.Notification, error) {
	row, err := s.getStreamRow(ctx, url)
	if err == sql.ErrNoRows {
		return nil, storage.ErrStreamNotFound
	} else if err != nil {
		return nil, err
	}

	if row.expiresAt != nil && time.Now().After(*row.expiresAt) {
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
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	close(s.cleanupDone)
	s.notifier.Close()

	return s.db.Close()
}

type streamRow struct {
	url         string
	contentType string
	createdAt   time.Time
	ttl         *time.Duration
	expiresAt   *time.Time
	tailOffset  string
	lastSeq     string
	isJSON      bool

	offsetLastTime     int64
	offsetLastSequence uint32
	offsetBytePosition uint64
}

func (s *Storage) getStreamRow(ctx context.Context, url string) (*streamRow, error) {
	var row streamRow
	var createdAtMs int64
	var ttlMs sql.NullInt64
	var expiresAtMs sql.NullInt64
	var lastSeq sql.NullString
	var isJSON int

	err := s.db.QueryRowContext(ctx, `
		SELECT url, content_type, created_at, ttl_ms, expires_at,
		       tail_offset, last_seq, is_json,
		       offset_last_time, offset_last_sequence, offset_byte_position
		FROM streams WHERE url = ?`, url,
	).Scan(
		&row.url, &row.contentType, &createdAtMs, &ttlMs, &expiresAtMs,
		&row.tailOffset, &lastSeq, &isJSON,
		&row.offsetLastTime, &row.offsetLastSequence, &row.offsetBytePosition,
	)
	if err != nil {
		return nil, err
	}

	row.createdAt = time.UnixMilli(createdAtMs)
	row.isJSON = isJSON == 1

	if ttlMs.Valid {
		ttl := time.Duration(ttlMs.Int64) * time.Millisecond
		row.ttl = &ttl
	}
	if expiresAtMs.Valid {
		exp := time.UnixMilli(expiresAtMs.Int64)
		row.expiresAt = &exp
	}
	if lastSeq.Valid {
		row.lastSeq = lastSeq.String
	}

	return &row, nil
}

func (s *Storage) getOrCreateGenerator(url string, row *streamRow) *stream.Generator {
	s.generatorsMu.RLock()
	gen, ok := s.generators[url]
	s.generatorsMu.RUnlock()

	if ok {
		return gen
	}

	s.generatorsMu.Lock()
	defer s.generatorsMu.Unlock()

	if gen, ok = s.generators[url]; ok {
		return gen
	}

	gen = stream.NewGenerator()
	gen.SetPosition(stream.Offset{
		Timestamp:    row.offsetLastTime,
		Sequence:     row.offsetLastSequence,
		BytePosition: row.offsetBytePosition,
	})

	s.generators[url] = gen
	return gen
}

func (s *Storage) setGenerator(url string, gen *stream.Generator) {
	s.generatorsMu.Lock()
	s.generators[url] = gen
	s.generatorsMu.Unlock()
}

func (s *Storage) removeGenerator(url string) {
	s.generatorsMu.Lock()
	delete(s.generators, url)
	s.generatorsMu.Unlock()
}

func (s *Storage) cleanupLoop() {
	ticker := time.NewTicker(s.opts.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.cleanupDone:
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

func (s *Storage) cleanupExpired() {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	rows, err := s.db.QueryContext(ctx,
		"SELECT url FROM streams WHERE expires_at IS NOT NULL AND expires_at <= ?",
		nowMs,
	)
	if err != nil {
		return
	}

	var expiredURLs []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err == nil {
			expiredURLs = append(expiredURLs, url)
		}
	}
	rows.Close()

	if len(expiredURLs) == 0 {
		return
	}

	s.writeMu.Lock()
	_, _ = s.db.ExecContext(ctx,
		"DELETE FROM streams WHERE expires_at IS NOT NULL AND expires_at <= ?",
		nowMs,
	)
	s.writeMu.Unlock()

	for _, url := range expiredURLs {
		s.notifier.NotifyError(url, storage.ErrStreamNotFound)
		s.removeGenerator(url)
	}
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

func expiresAtEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}
