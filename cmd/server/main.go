package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"ella.to/bus/server"
)

func main() {
	ctx := context.TODO()

	const testDbPath = "./bus.db"

	os.Remove(testDbPath)
	os.Remove(testDbPath + "-shm")
	os.Remove(testDbPath + "-wal")

	slog.SetLogLoggerLevel(slog.LevelDebug)

	addr := "0.0.0.0:2021"

	handler, err := server.New(
		ctx,
		// server.WithStorageMemory(),
		server.WithStoragePath("./bus.db"),
		server.WithStoragePoolSize(10),
	)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	server := http.Server{
		Handler: handler,
		Addr:    addr,
	}
	defer server.Close()

	slog.Info("starting server", "addr", addr)

	err = server.ListenAndServe()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
