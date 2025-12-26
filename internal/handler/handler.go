package handler

import (
	"net/http"
	"time"

	"github.com/tinyadmin/durable-go/internal/auth"
	"github.com/tinyadmin/durable-go/internal/cursor"
	"github.com/tinyadmin/durable-go/storage"
)

// Handler handles all HTTP requests for the durable streams server.
type Handler struct {
	storage storage.Storage
	cursor  *cursor.Generator
	auth    auth.AuthProvider // nil = no auth

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

// WithAuth sets the auth provider and returns the handler for chaining.
func (h *Handler) WithAuth(provider auth.AuthProvider) *Handler {
	h.auth = provider
	return h
}

// scopedStreamURL returns the stream URL scoped to the tenant if auth is enabled.
func (h *Handler) scopedStreamURL(r *http.Request) string {
	path := r.URL.Path
	if authCtx := auth.FromContext(r.Context()); authCtx != nil && authCtx.TenantID != "" {
		return "/t/" + authCtx.TenantID + path
	}
	return path
}

// authorize checks if the operation is allowed. Returns true if allowed.
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, op auth.Operation, streamURL string) bool {
	if h.auth == nil {
		return true
	}
	authCtx := auth.FromContext(r.Context())
	if err := h.auth.Authorize(authCtx, op, streamURL); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	return true
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

	// Authenticate if provider configured
	if h.auth != nil {
		authCtx, err := h.auth.Authenticate(r)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		r = r.WithContext(auth.WithContext(r.Context(), authCtx))
	}

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
