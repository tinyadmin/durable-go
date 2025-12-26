package handler

import (
	"errors"
	"io"
	"net/http"

	"github.com/me/durable/internal/auth"
	"github.com/me/durable/internal/storage"
	"github.com/me/durable/internal/stream"
)

// handleAppend handles POST requests to append data to a stream.
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	streamURL := h.scopedStreamURL(r)

	// Authorize
	if !h.authorize(w, r, auth.OpAppend, streamURL) {
		return
	}

	// Content-Type is required for append
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		http.Error(w, "Content-Type required", http.StatusBadRequest)
		return
	}

	// Read body
	body, err := io.ReadAll(io.LimitReader(r.Body, h.MaxBodySize))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	if len(body) == 0 {
		http.Error(w, "Empty body not allowed", http.StatusBadRequest)
		return
	}

	opts := storage.AppendOptions{
		ContentType: contentType,
		SeqNumber:   r.Header.Get("Stream-Seq"),
	}

	// Append to stream
	result, err := h.storage.Append(ctx, streamURL, body, opts)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrStreamNotFound):
			http.Error(w, "Stream not found", http.StatusNotFound)
		case errors.Is(err, storage.ErrContentTypeMismatch):
			http.Error(w, "Content-Type does not match stream", http.StatusConflict)
		case errors.Is(err, storage.ErrSequenceRegression):
			http.Error(w, "Sequence number regression", http.StatusConflict)
		case errors.Is(err, storage.ErrEmptyAppend):
			http.Error(w, "Empty append not allowed", http.StatusBadRequest)
		case errors.Is(err, stream.ErrInvalidJSON):
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
		case errors.Is(err, stream.ErrEmptyArray):
			http.Error(w, "Empty array not allowed", http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Set response headers
	w.Header().Set("Stream-Next-Offset", result.NextOffset)
	w.WriteHeader(http.StatusNoContent)
}
