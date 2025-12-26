package handler

import (
	"errors"
	"net/http"

	"github.com/me/durable/internal/storage"
)

// handleMetadata handles HEAD requests to get stream metadata.
func (h *Handler) handleMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	streamURL := r.URL.Path

	meta, err := h.storage.GetMetadata(ctx, streamURL)
	if err != nil {
		if errors.Is(err, storage.ErrStreamNotFound) {
			http.Error(w, "Stream not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Stream-Next-Offset", meta.TailOffset)

	// Avoid stale tail offsets
	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(http.StatusOK)
}
