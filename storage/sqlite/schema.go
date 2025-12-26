package sqlite

import "database/sql"

const schemaSQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS streams (
    url TEXT PRIMARY KEY,
    content_type TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    ttl_ms INTEGER,
    expires_at INTEGER,
    tail_offset TEXT NOT NULL,
    last_seq TEXT,
    is_json INTEGER NOT NULL DEFAULT 0,
    offset_last_time INTEGER NOT NULL DEFAULT 0,
    offset_last_sequence INTEGER NOT NULL DEFAULT 0,
    offset_byte_position INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_url TEXT NOT NULL,
    offset TEXT NOT NULL,
    data BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (stream_url) REFERENCES streams(url) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_messages_stream_offset ON messages(stream_url, offset);
CREATE INDEX IF NOT EXISTS idx_streams_expires_at ON streams(expires_at) WHERE expires_at IS NOT NULL;
`

func initSchema(db *sql.DB) error {
	_, err := db.Exec(schemaSQL)
	return err
}
