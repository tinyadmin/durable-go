package handler

import "net/http"

const (
	corsAllowOrigin  = "*"
	corsAllowMethods = "GET, POST, PUT, DELETE, HEAD, OPTIONS"
	corsAllowHeaders = "Content-Type, Authorization, Stream-Seq, Stream-TTL, Stream-Expires-At"
	corsExposeHeaders = "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, ETag, Content-Type, Content-Encoding, Vary"
	corsMaxAge       = "86400" // 24 hours
)

// handleCORS handles OPTIONS preflight requests.
func (h *Handler) handleCORS(w http.ResponseWriter, r *http.Request) {
	h.setCORSHeaders(w)
	w.Header().Set("Access-Control-Max-Age", corsMaxAge)
	w.WriteHeader(http.StatusNoContent)
}

// setCORSHeaders sets CORS headers on the response.
func (h *Handler) setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", corsAllowOrigin)
	w.Header().Set("Access-Control-Allow-Methods", corsAllowMethods)
	w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
	w.Header().Set("Access-Control-Expose-Headers", corsExposeHeaders)
}
