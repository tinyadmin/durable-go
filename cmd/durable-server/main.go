package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/me/durable/internal/handler"
	"github.com/me/durable/internal/storage"
	"github.com/me/durable/internal/storage/memory"
)

func main() {
	// Parse flags
	port := flag.Int("port", 4437, "Port to listen on")
	host := flag.String("host", "127.0.0.1", "Host to bind to")
	storageType := flag.String("storage", "memory", "Storage backend (memory, sqlite, s3)")
	flag.Parse()

	// Create storage backend
	var store storage.Storage
	switch *storageType {
	case "memory":
		store = memory.New()
	case "sqlite":
		log.Fatal("SQLite storage not yet implemented")
	case "s3":
		log.Fatal("S3 storage not yet implemented")
	default:
		log.Fatalf("Unknown storage type: %s", *storageType)
	}
	defer store.Close()

	// Create handler
	h := handler.New(store)

	// Create server
	addr := fmt.Sprintf("%s:%d", *host, *port)
	server := &http.Server{
		Addr:         addr,
		Handler:      h,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Durable Streams server listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped")
}
