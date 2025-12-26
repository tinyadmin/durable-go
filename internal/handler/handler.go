package handler

import (
	"net/http"
	"time"

	"github.com/me/durable/internal/cursor"
	"github.com/me/durable/internal/storage"
)

// Handler handles all HTTP requests for the durable streams server.
type Handler struct {
	storage storage.Storage
	cursor  *cursor.Generator

	// Configuration
	LongPollTimeout time.Duration
	MaxBodySize     int64
}

// New creates a new handler with the given storage backend.
func New(store storage.Storage) *Handler {
	return &Handler{
		storage:         store,
		cursor:          cursor.New(),
		LongPollTimeout: 30 * time.Second,
		MaxBodySize:     10 * 1024 * 1024, // 10MB
	}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS preflight
	if r.Method == http.MethodOptions {
		h.handleCORS(w, r)
		return
	}

	// Add CORS headers to all responses
	h.setCORSHeaders(w)

	// Route by method
	switch r.Method {
	case http.MethodPut:
		h.handleCreate(w, r)
	case http.MethodPost:
		h.handleAppend(w, r)
	case http.MethodGet:
		h.handleRead(w, r)
	case http.MethodHead:
		h.handleMetadata(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
