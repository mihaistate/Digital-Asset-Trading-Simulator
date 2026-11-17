package main

import (
	"context"
	"example.com/digital-asset-simulator/internal/engine"
	"example.com/digital-asset-simulator/internal/feed"
	"example.com/digital-asset-simulator/internal/httpapi"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	mode := env("FEED_MODE", "demo")
	if mode != "demo" && mode != "coinbase" && mode != "replay" {
		logger.Error("FEED_MODE must be demo, coinbase or replay")
		os.Exit(1)
	}
	e := engine.New(engine.DefaultConfig(), mode)
	switch mode {
	case "demo":
		go feed.Demo(ctx, e)
	case "coinbase":
		go feed.Coinbase(ctx, e)
	case "replay":
		path := os.Getenv("REPLAY_FILE")
		if path == "" {
			logger.Error("REPLAY_FILE is required")
			os.Exit(1)
		}
		go func() {
			if err := feed.Replay(ctx, e, path, 250*time.Millisecond); err != nil {
				e.SetConnection(false, err.Error())
				logger.Error("replay failed", "error", err)
			}
		}()
	}
	server := &http.Server{Addr: env("HTTP_ADDR", "127.0.0.1:8080"), Handler: httpapi.Handler(e, os.Getenv("API_TOKEN")), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	go func() {
		logger.Info("paper trading server started", "addr", server.Addr, "feed", mode)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		logger.Error("shutdown", "error", err)
	}
}
