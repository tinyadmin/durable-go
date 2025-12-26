package rediss3

import "time"

// Options configures the Redis+S3 storage backend.
type Options struct {
	// Redis configuration
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// S3 configuration (optional - for archival)
	S3Bucket   string
	S3Endpoint string // For MinIO/R2 compatibility
	S3Region   string
	PathStyle  bool // Required for MinIO

	// Tiering configuration
	ArchiveAfter    time.Duration // Move messages to S3 after this age (0 = disabled)
	MaxRedisBytes   int64         // Max bytes per stream in Redis before archiving (0 = unlimited)
	CleanupInterval time.Duration // How often to check for expired streams
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		RedisAddr:       "localhost:6379",
		RedisDB:         0,
		S3Region:        "us-east-1",
		ArchiveAfter:    0, // Disabled by default
		CleanupInterval: 1 * time.Minute,
	}
}
