package handler

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/me/durable/internal/storage"
	"github.com/me/durable/internal/stream"
)

// handleCreate handles PUT requests to create a stream.
func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	streamURL := r.URL.Path

	// Parse headers
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	opts := storage.CreateOptions{
		ContentType: contentType,
	}

	// Parse TTL
	if ttlStr := r.Header.Get("Stream-TTL"); ttlStr != "" {
		ttl, err := parseTTL(ttlStr)
		if err != nil {
			http.Error(w, "Invalid Stream-TTL", http.StatusBadRequest)
			return
		}
		opts.TTL = &ttl
	}

	// Parse Expires-At
	if expiresStr := r.Header.Get("Stream-Expires-At"); expiresStr != "" {
		expires, err := time.Parse(time.RFC3339, expiresStr)
		if err != nil {
			http.Error(w, "Invalid Stream-Expires-At", http.StatusBadRequest)
			return
		}
		opts.ExpiresAt = &expires
	}

	// Cannot have both TTL and Expires-At
	if opts.TTL != nil && opts.ExpiresAt != nil {
		http.Error(w, "Cannot specify both Stream-TTL and Stream-Expires-At", http.StatusBadRequest)
		return
	}

	// Read optional body
	if r.ContentLength > 0 || r.ContentLength == -1 {
		body, err := io.ReadAll(io.LimitReader(r.Body, h.MaxBodySize))
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		if len(body) > 0 {
			opts.InitialData = body
		}
	}

	// Create stream
	meta, created, err := h.storage.CreateStream(ctx, streamURL, opts)
	if err != nil {
		if errors.Is(err, storage.ErrStreamConfigMismatch) {
			http.Error(w, "Stream exists with different configuration", http.StatusConflict)
			return
		}
		if errors.Is(err, stream.ErrInvalidJSON) {
			http.Error(w, "Invalid JSON in body", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Stream-Next-Offset", meta.TailOffset)

	if created {
		// Location header should be absolute URL
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		location := scheme + "://" + r.Host + streamURL
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

// parseTTL parses a TTL string (non-negative integer, no leading zeros, no plus sign).
func parseTTL(s string) (time.Duration, error) {
	if len(s) == 0 {
		return 0, errors.New("empty TTL")
	}

	// Reject plus sign prefix
	if s[0] == '+' {
		return 0, errors.New("plus sign not allowed")
	}

	// Check for leading zeros (except "0" itself)
	if len(s) > 1 && s[0] == '0' {
		return 0, errors.New("leading zeros not allowed")
	}

	seconds, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	if seconds < 0 {
		return 0, errors.New("negative TTL not allowed")
	}

	return time.Duration(seconds) * time.Second, nil
}
