package handler

import (
	"errors"
	"io"
	"net/http"

	"github.com/me/durable/internal/auth"
	"github.com/me/durable/storage"
	"github.com/me/durable/stream"
)

// handleRead handles GET requests to read from a stream.
func (h *Handler) handleRead(w http.ResponseWriter, r *http.Request) {
	streamURL := h.scopedStreamURL(r)

	// Authorize
	if !h.authorize(w, r, auth.OpRead, streamURL) {
		return
	}

	query := r.URL.Query()

	// Check for multiple offset params (not allowed)
	if len(query["offset"]) > 1 {
		http.Error(w, "Multiple offset parameters not allowed", http.StatusBadRequest)
		return
	}

	// Check if offset param exists but is empty
	if _, hasOffset := query["offset"]; hasOffset && query.Get("offset") == "" {
		http.Error(w, "Empty offset not allowed", http.StatusBadRequest)
		return
	}

	// Parse offset
	offset := query.Get("offset")

	// Validate offset format
	if offset != "" && offset != "-1" {
		if _, err := stream.Parse(offset); err != nil {
			http.Error(w, "Invalid offset", http.StatusBadRequest)
			return
		}
	}

	// Route based on live mode
	liveMode := query.Get("live")
	clientCursor := query.Get("cursor")

	switch liveMode {
	case "":
		h.handleCatchUpRead(w, r, streamURL, offset)
	case "long-poll":
		// Long-poll requires offset parameter
		if offset == "" {
			http.Error(w, "Offset required for long-poll", http.StatusBadRequest)
			return
		}
		h.handleLongPoll(w, r, streamURL, offset, clientCursor)
	case "sse":
		// SSE requires offset parameter
		if offset == "" {
			http.Error(w, "Offset required for SSE", http.StatusBadRequest)
			return
		}
		h.handleSSE(w, r, streamURL, offset, clientCursor)
	default:
		http.Error(w, "Invalid live mode", http.StatusBadRequest)
	}
}

// handleCatchUpRead handles regular GET requests (catch-up reads).
func (h *Handler) handleCatchUpRead(w http.ResponseWriter, r *http.Request, streamURL, offset string) {
	ctx := r.Context()

	result, err := h.storage.Read(ctx, streamURL, offset)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrStreamNotFound):
			http.Error(w, "Stream not found", http.StatusNotFound)
		case errors.Is(err, stream.ErrInvalidOffset):
			http.Error(w, "Invalid offset", http.StatusBadRequest)
		case errors.Is(err, storage.ErrOffsetNotFound):
			http.Error(w, "Offset not found or expired", http.StatusGone)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer result.Data.Close()

	// Read data for ETag calculation
	data, err := io.ReadAll(result.Data)
	if err != nil {
		http.Error(w, "Failed to read data", http.StatusInternalServerError)
		return
	}

	// Generate ETag based on stream URL and offset range
	startOffset := offset
	if startOffset == "" {
		startOffset = "-1"
	}
	etag := generateETag(streamURL, startOffset, result.NextOffset)

	// Check If-None-Match
	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == etag || match == "\""+etag+"\"" {
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Set response headers
	w.Header().Set("Content-Type", result.ContentType)
	w.Header().Set("Stream-Next-Offset", result.NextOffset)
	w.Header().Set("ETag", etag)

	if result.UpToDate {
		w.Header().Set("Stream-Up-To-Date", "true")
	}

	// Cache control for CDN
	w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

	// Write data
	w.Write(data)
}

// generateETag creates an ETag based on stream URL and offset range.
func generateETag(streamURL, startOffset, endOffset string) string {
	// Simple ETag format: hash of url:start:end
	// For simplicity, just use the offsets directly
	return startOffset + ":" + endOffset
}
