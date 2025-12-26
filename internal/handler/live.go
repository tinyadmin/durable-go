package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/tinyadmin/durable-go/storage"
	"github.com/tinyadmin/durable-go/stream"
)

// handleLongPoll handles GET requests with ?live=long-poll.
func (h *Handler) handleLongPoll(w http.ResponseWriter, r *http.Request, streamURL, offset, clientCursor string) {
	ctx := r.Context()

	// Set timeout
	ctx, cancel := context.WithTimeout(ctx, h.LongPollTimeout)
	defer cancel()

	// Calculate cursor for CDN collapsing
	responseCursor := h.cursor.Next(clientCursor)

	// First, try immediate read
	result, err := h.storage.Read(ctx, streamURL, offset)
	if err != nil {
		h.handleReadError(w, err)
		return
	}

	// Check if data is available
	data, err := io.ReadAll(result.Data)
	result.Data.Close()
	if err != nil {
		http.Error(w, "Failed to read data", http.StatusInternalServerError)
		return
	}

	// For JSON mode, check if response is empty array
	isEmptyJSON := storage.IsJSONContentType(result.ContentType) && string(data) == "[]"

	if len(data) > 0 && !isEmptyJSON {
		// Data available, return immediately
		w.Header().Set("Content-Type", result.ContentType)
		w.Header().Set("Stream-Next-Offset", result.NextOffset)
		w.Header().Set("Stream-Cursor", responseCursor)
		if result.UpToDate {
			w.Header().Set("Stream-Up-To-Date", "true")
		}
		w.Write(data)
		return
	}

	// No data yet, subscribe for updates
	notifications, err := h.storage.Subscribe(ctx, streamURL, offset)
	if err != nil {
		h.handleReadError(w, err)
		return
	}
	defer h.storage.Unsubscribe(ctx, streamURL, notifications)

	select {
	case <-ctx.Done():
		// Timeout - return 204 No Content
		w.Header().Set("Stream-Next-Offset", result.NextOffset)
		w.Header().Set("Stream-Up-To-Date", "true")
		w.Header().Set("Stream-Cursor", responseCursor)
		w.WriteHeader(http.StatusNoContent)

	case notification := <-notifications:
		if notification.Error != nil {
			h.handleReadError(w, notification.Error)
			return
		}

		// New data available, fetch and return
		newResult, err := h.storage.Read(ctx, streamURL, offset)
		if err != nil {
			h.handleReadError(w, err)
			return
		}
		defer newResult.Data.Close()

		w.Header().Set("Content-Type", newResult.ContentType)
		w.Header().Set("Stream-Next-Offset", newResult.NextOffset)
		w.Header().Set("Stream-Cursor", responseCursor)
		if newResult.UpToDate {
			w.Header().Set("Stream-Up-To-Date", "true")
		}
		io.Copy(w, newResult.Data)
	}
}

// handleSSE handles GET requests with ?live=sse.
func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, streamURL, offset, clientCursor string) {
	ctx := r.Context()

	// Get metadata to validate content type
	meta, err := h.storage.GetMetadata(ctx, streamURL)
	if err != nil {
		h.handleReadError(w, err)
		return
	}

	// SSE only supports text/* or application/json
	if !isSSECompatible(meta.ContentType) {
		http.Error(w, "SSE only supports text/* or application/json", http.StatusBadRequest)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Calculate cursor for CDN collapsing (with jitter if collision)
	responseCursor := h.cursor.Next(clientCursor)

	// Connection close timer (~55-65 seconds for CDN collapsing)
	connectionDuration := 55*time.Second + time.Duration(rand.Intn(10))*time.Second
	connectionTimer := time.NewTimer(connectionDuration)
	defer connectionTimer.Stop()

	currentOffset := offset

	for {
		select {
		case <-ctx.Done():
			return

		case <-connectionTimer.C:
			// Close connection for CDN collapsing
			return

		default:
			// Read available data
			result, err := h.storage.Read(ctx, streamURL, currentOffset)
			if err != nil {
				h.sendSSEError(w, flusher, err)
				return
			}

			data, _ := io.ReadAll(result.Data)
			result.Data.Close()

			// For JSON mode, check if response is empty array
			isEmptyJSON := storage.IsJSONContentType(result.ContentType) && string(data) == "[]"

			if len(data) > 0 && !isEmptyJSON {
				// Send data event
				h.sendSSEData(w, flusher, data)

				// Send control event with calculated cursor
				h.sendSSEControl(w, flusher, result.NextOffset, responseCursor, result.UpToDate)

				currentOffset = result.NextOffset
			} else if result.UpToDate {
				// Send control event indicating up-to-date
				h.sendSSEControl(w, flusher, result.NextOffset, responseCursor, true)
			}

			if result.UpToDate {
				// No more data, wait for notification
				notifications, err := h.storage.Subscribe(ctx, streamURL, currentOffset)
				if err != nil {
					h.sendSSEError(w, flusher, err)
					return
				}

				select {
				case <-ctx.Done():
					h.storage.Unsubscribe(ctx, streamURL, notifications)
					return
				case <-connectionTimer.C:
					h.storage.Unsubscribe(ctx, streamURL, notifications)
					return
				case n := <-notifications:
					h.storage.Unsubscribe(ctx, streamURL, notifications)
					if n.Error != nil {
						h.sendSSEError(w, flusher, n.Error)
						return
					}
					// Loop will read new data
				}
			}
		}
	}
}

func (h *Handler) sendSSEData(w http.ResponseWriter, f http.Flusher, data []byte) {
	fmt.Fprintf(w, "event: data\n")

	// Split data by newlines for proper SSE format
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	fmt.Fprintf(w, "\n")
	f.Flush()
}

func (h *Handler) sendSSEControl(w http.ResponseWriter, f http.Flusher, offset, cursor string, upToDate bool) {
	control := map[string]interface{}{
		"streamNextOffset": offset,
		"streamCursor":     cursor,
	}
	if upToDate {
		control["upToDate"] = true
	}
	data, _ := json.Marshal(control)

	fmt.Fprintf(w, "event: control\n")
	fmt.Fprintf(w, "data: %s\n", data)
	fmt.Fprintf(w, "\n")
	f.Flush()
}

func (h *Handler) sendSSEError(w http.ResponseWriter, f http.Flusher, err error) {
	errData := map[string]string{"error": err.Error()}
	data, _ := json.Marshal(errData)

	fmt.Fprintf(w, "event: error\n")
	fmt.Fprintf(w, "data: %s\n", data)
	fmt.Fprintf(w, "\n")
	f.Flush()
}

func (h *Handler) handleReadError(w http.ResponseWriter, err error) {
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
}

func isSSECompatible(contentType string) bool {
	ct := storage.NormalizeContentType(contentType)
	return strings.HasPrefix(ct, "text/") || ct == "application/json"
}
