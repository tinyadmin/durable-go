package handler

import (
	"errors"
	"net/http"

	"github.com/me/durable/internal/storage"
)

// handleDelete handles DELETE requests to remove a stream.
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	streamURL := r.URL.Path

	err := h.storage.DeleteStream(ctx, streamURL)
	if err != nil {
		if errors.Is(err, storage.ErrStreamNotFound) {
			http.Error(w, "Stream not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
