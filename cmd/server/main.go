package main

import (
	"context"
	"log/slog"
	"net/http"

	"ella.to/bus/server"
)

const (
	defaultAddr = "0.0.0.0:2021"
)

func main() {
	ctx := context.Background()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	handler, err := server.New(
		ctx,
		// server.WithDBPath("bus.db"),
		// server.WithEventsDeleteInterval(10*time.Hour),
	)
	if err != nil {
		slog.Error("failed to initilaize server", "error", err.Error())
		return
	}

	server := http.Server{
		Handler: handler,
		Addr:    defaultAddr,
	}
	defer server.Close()

	slog.Info("starting server", "addr", defaultAddr)

	err = server.ListenAndServe()
	if err != nil {
		slog.Error(err.Error())
		return
	}
}
