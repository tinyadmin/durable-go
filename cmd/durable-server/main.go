package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tinyadmin/durable-go/internal/handler"
	"github.com/tinyadmin/durable-go/storage"
	"github.com/tinyadmin/durable-go/storage/memory"
)

func main() {
	// Parse flags
	port := flag.Int("port", 4437, "Port to listen on")
	host := flag.String("host", "127.0.0.1", "Host to bind to")
	storageType := flag.String("storage", "memory", "Storage backend (memory)")
	flag.Parse()

	// Create storage backend
	var store storage.Storage
	switch *storageType {
	case "memory":
		store = memory.New()
	default:
		slog.Error("unknown storage type", "type", *storageType, "hint", "sqlite available as separate module")
		os.Exit(1)
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
		slog.Info("server started", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("server forced to shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("server stopped")
}
