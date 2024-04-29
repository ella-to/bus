package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"ella.to/bus/server"
)

var Version = "master"
var GitCommit = "development"

func getLogLevel() slog.Level {
	value := os.Getenv("BUS_LOG_LEVEL")
	if value == "" {
		return slog.LevelInfo
	}

	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func getServerAddr() string {
	value := os.Getenv("BUS_SERVER_ADDR")
	if value == "" {
		return server.DefaultAddr
	}

	return value
}

func getStoragePoolSize(serverOpts []server.Opt) []server.Opt {
	value, err := strconv.ParseInt(os.Getenv("BUS_STORAGE_POOL_SIZE"), 10, 64)
	if err != nil {
		return serverOpts
	}

	return append(serverOpts, server.WithStoragePoolSize(int(value)))
}

func getStoragePath(serverOpts []server.Opt) []server.Opt {
	value := os.Getenv("BUS_STORAGE_PATH")
	if value == "" {
		return serverOpts
	}

	return append(serverOpts, server.WithStoragePath(value))
}

func getWorkerBufferSize(serverOpts []server.Opt) []server.Opt {
	value, err := strconv.ParseInt(os.Getenv("BUS_WORKER_BUFFER_SIZE"), 10, 64)
	if err != nil {
		return serverOpts
	}

	return append(serverOpts, server.WithWorkerBufferSize(value))
}

func getCleanExpiredEventsFreq(serverOpts []server.Opt) []server.Opt {
	value, err := time.ParseDuration(os.Getenv("BUS_CLEAN_EXPIRED_EVENTS_FREQ"))
	if err != nil {
		return serverOpts
	}

	return append(serverOpts, server.WithCleanExpiredEventsFreq(value))
}

func main() {
	ctx := context.TODO()

	slog.SetLogLoggerLevel(getLogLevel())

	addr := getServerAddr()

	serverOpts := []server.Opt{}

	serverOpts = getStoragePoolSize(serverOpts)
	serverOpts = getStoragePath(serverOpts)
	serverOpts = getWorkerBufferSize(serverOpts)
	serverOpts = getCleanExpiredEventsFreq(serverOpts)

	handler, err := server.New(
		ctx,
		serverOpts...,
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

	fmt.Printf(`
	Version: %s
	GitCommit: %s
		`, Version, GitCommit)

	slog.Info("starting server", "addr", addr)

	err = server.ListenAndServe()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
