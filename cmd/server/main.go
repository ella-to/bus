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
	"ella.to/bus/storage"
)

var Version = "main"
var GitCommit = "development"

const (
	logoAscii = `
██████╗░██╗░░░██╗░██████╗
██╔══██╗██║░░░██║██╔════╝
██████╦╝██║░░░██║╚█████╗░
██╔══██╗██║░░░██║░╚═══██╗
██████╦╝╚██████╔╝██████╔╝
╚═════╝░░╚═════╝░╚═════╝░`
)

func printLogo() {
	fmt.Println(logoAscii)
	fmt.Printf("Version: %s\n", Version)
	fmt.Printf("Git Commit: %s\n", GitCommit)
	fmt.Println()
}

func getSqlitePath(defaultValue string) string {
	value, ok := os.LookupEnv("BUS_SQLITE_PATH")
	if !ok {
		return defaultValue
	}
	return value
}

func getActionBufferSize(defaultValue int) int {
	value, ok := os.LookupEnv("BUS_ACTION_BUFFER_SIZE")
	if !ok {
		return defaultValue
	}
	bufferSize, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}
	return int(bufferSize)
}

func getActionPoolSize(defaultValue int) int {
	value, ok := os.LookupEnv("BUS_ACTION_POOL_SIZE")
	if !ok {
		return defaultValue
	}
	poolSize, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}
	return int(poolSize)
}

func getDeleteEventsFreq(defaultValue time.Duration) time.Duration {
	value, ok := os.LookupEnv("BUS_DELETE_EVENTS_FREQ")
	if !ok {
		return defaultValue
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return duration
}

func getAddr(defaultValue string) string {
	value, ok := os.LookupEnv("BUS_ADDR")
	if !ok {
		return defaultValue
	}
	return value
}

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

func main() {
	slog.SetLogLoggerLevel(getLogLevel())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	printLogo()

	var (
		sqlitePath       = getSqlitePath("")                     // empty string means in-memory database
		actionBufferSize = getActionBufferSize(100)              // how many actions can be buffered
		actionPoolSize   = getActionPoolSize(100)                // how many actions objects should be created in the pool
		deleteEventsFreq = getDeleteEventsFreq(30 * time.Second) // how often to delete expired events
		addr             = getAddr("0.0.0.0:2021")               // address to listen on
	)

	storage, err := storage.NewSqlite(ctx, sqlitePath)
	if err != nil {
		slog.Error("failed to create sqlite storage", "error", err)
		return
	}

	busServer := server.New(
		ctx,
		storage,
		server.WithActionBufferSize(actionBufferSize),
		server.WithActionPoolSize(actionPoolSize),
		server.WithDeleteEventFrequency(deleteEventsFreq),
	)
	defer busServer.Close()

	server := http.Server{
		Handler: busServer,
		Addr:    addr,
	}
	defer server.Close()

	slog.Info("starting server", "addr", addr)

	err = server.ListenAndServe()
	if err != nil {
		slog.Error("failed to start server", "error", err)
	}

}
