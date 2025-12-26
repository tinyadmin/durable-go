package test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tinyadmin/durable-go/internal/handler"
	"github.com/tinyadmin/durable-go/storage/memory"
)

func TestStreamLifecycle(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/test-stream"

	// Test: Create stream
	t.Run("CreateStream", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", streamURL, nil)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected 201, got %d", resp.StatusCode)
		}

		if resp.Header.Get("Stream-Next-Offset") == "" {
			t.Error("Expected Stream-Next-Offset header")
		}
	})

	// Test: Idempotent create
	t.Run("IdempotentCreate", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", streamURL, nil)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected 200 for idempotent create, got %d", resp.StatusCode)
		}
	})

	// Test: Append data
	var nextOffset string
	t.Run("AppendData", func(t *testing.T) {
		body := bytes.NewBufferString(`{"event": "test"}`)
		req, _ := http.NewRequest("POST", streamURL, body)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("Expected 204, got %d: %s", resp.StatusCode, body)
		}

		nextOffset = resp.Header.Get("Stream-Next-Offset")
		if nextOffset == "" {
			t.Error("Expected Stream-Next-Offset header")
		}
	})

	// Test: Read data
	t.Run("ReadData", func(t *testing.T) {
		req, _ := http.NewRequest("GET", streamURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected 200, got %d", resp.StatusCode)
		}

		data, _ := io.ReadAll(resp.Body)
		var arr []map[string]interface{}
		if err := json.Unmarshal(data, &arr); err != nil {
			t.Errorf("Failed to parse JSON: %v", err)
		}

		if len(arr) != 1 || arr[0]["event"] != "test" {
			t.Errorf("Unexpected data: %s", data)
		}

		if resp.Header.Get("Stream-Up-To-Date") != "true" {
			t.Error("Expected Stream-Up-To-Date: true")
		}
	})

	// Test: HEAD metadata
	t.Run("HeadMetadata", func(t *testing.T) {
		req, _ := http.NewRequest("HEAD", streamURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to get metadata: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected 200, got %d", resp.StatusCode)
		}

		if resp.Header.Get("Stream-Next-Offset") != nextOffset {
			t.Errorf("Expected offset %s, got %s", nextOffset, resp.Header.Get("Stream-Next-Offset"))
		}
	})

	// Test: Delete stream
	t.Run("DeleteStream", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", streamURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("Expected 204, got %d", resp.StatusCode)
		}
	})

	// Test: Stream not found after delete
	t.Run("StreamNotFoundAfterDelete", func(t *testing.T) {
		req, _ := http.NewRequest("GET", streamURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected 404, got %d", resp.StatusCode)
		}
	})
}

func TestJSONArrayFlattening(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/json-test"

	// Create stream
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req)

	// Append array - should be flattened into 2 messages
	body := bytes.NewBufferString(`[{"id": 1}, {"id": 2}]`)
	req, _ = http.NewRequest("POST", streamURL, body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", resp.StatusCode)
	}

	// Read back - array flattened, response wrapped in outer array
	req, _ = http.NewRequest("GET", streamURL, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	expected := `[{"id":1},{"id":2}]`
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, data)
	}
}

func TestByteStreamMode(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/bytes-test"

	// Create stream with text/plain
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "text/plain")
	http.DefaultClient.Do(req)

	// Append data
	req, _ = http.NewRequest("POST", streamURL, bytes.NewBufferString("hello"))
	req.Header.Set("Content-Type", "text/plain")
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest("POST", streamURL, bytes.NewBufferString("world"))
	req.Header.Set("Content-Type", "text/plain")
	http.DefaultClient.Do(req)

	// Read back
	req, _ = http.NewRequest("GET", streamURL, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if string(data) != "helloworld" {
		t.Errorf("Expected 'helloworld', got '%s'", data)
	}
}

func TestContentTypeMismatch(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/type-test"

	// Create stream with JSON
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req)

	// Try to append with wrong content type
	req, _ = http.NewRequest("POST", streamURL, bytes.NewBufferString("test"))
	req.Header.Set("Content-Type", "text/plain")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("Expected 409, got %d", resp.StatusCode)
	}
}

func TestSequenceNumber(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/seq-test"

	// Create stream
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req)

	// Append with seq 001
	body := bytes.NewBufferString(`{"n": 1}`)
	req, _ = http.NewRequest("POST", streamURL, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-Seq", "001")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", resp.StatusCode)
	}

	// Append with seq 002 (should succeed)
	body = bytes.NewBufferString(`{"n": 2}`)
	req, _ = http.NewRequest("POST", streamURL, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-Seq", "002")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", resp.StatusCode)
	}

	// Append with seq 001 again (should fail - regression)
	body = bytes.NewBufferString(`{"n": 3}`)
	req, _ = http.NewRequest("POST", streamURL, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stream-Seq", "001")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("Expected 409 for seq regression, got %d", resp.StatusCode)
	}
}

func TestOffsetBasedReading(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/offset-test"

	// Create stream
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req)

	// Append multiple messages
	var offsets []string
	for i := 1; i <= 3; i++ {
		body := bytes.NewBufferString(`{"n": ` + string(rune('0'+i)) + `}`)
		req, _ = http.NewRequest("POST", streamURL, body)
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		offsets = append(offsets, resp.Header.Get("Stream-Next-Offset"))
		resp.Body.Close()
	}

	// Read from offset 0 (should get first message)
	req, _ = http.NewRequest("GET", streamURL+"?offset="+offsets[0], nil)
	resp, _ := http.DefaultClient.Do(req)
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var arr []map[string]interface{}
	json.Unmarshal(data, &arr)
	if len(arr) != 2 { // Should get messages 2 and 3
		t.Errorf("Expected 2 messages after offset, got %d", len(arr))
	}
}

func TestCORSHeaders(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	// Test OPTIONS preflight
	req, _ := http.NewRequest("OPTIONS", server.URL+"/test", nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204 for OPTIONS, got %d", resp.StatusCode)
	}

	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Expected Access-Control-Allow-Origin: *")
	}

	if !strings.Contains(resp.Header.Get("Access-Control-Allow-Methods"), "GET") {
		t.Error("Expected Allow-Methods to include GET")
	}
}

func TestLongPollTimeout(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	h.LongPollTimeout = 100 * time.Millisecond // Short timeout for test
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/longpoll-test"

	// Create stream
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	initialOffset := resp.Header.Get("Stream-Next-Offset")
	resp.Body.Close()

	// Long poll should timeout with 204
	start := time.Now()
	req, _ = http.NewRequest("GET", streamURL+"?offset="+initialOffset+"&live=long-poll", nil)
	resp, _ = http.DefaultClient.Do(req)
	elapsed := time.Since(start)
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204 on timeout, got %d", resp.StatusCode)
	}

	if elapsed < 90*time.Millisecond {
		t.Errorf("Long poll returned too quickly: %v", elapsed)
	}

	if resp.Header.Get("Stream-Up-To-Date") != "true" {
		t.Error("Expected Stream-Up-To-Date: true on timeout")
	}
}

func TestEmptyArrayReject(t *testing.T) {
	store := memory.New()
	defer store.Close()

	h := handler.New(store)
	server := httptest.NewServer(h)
	defer server.Close()

	streamURL := server.URL + "/empty-array-test"

	// Create stream
	req, _ := http.NewRequest("PUT", streamURL, nil)
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req)

	// Try to append empty array
	body := bytes.NewBufferString(`[]`)
	req, _ = http.NewRequest("POST", streamURL, body)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected 400 for empty array, got %d", resp.StatusCode)
	}
}
