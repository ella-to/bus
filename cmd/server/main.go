package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"ella.to/bus/server"
)

var Version = "master"
var GitCommit = "development"

func getEnvString(key string) (string, bool) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}

	return value, true
}

func getEnvInt(key string) (int, bool) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return 0, false
	}

	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		panic("expect integer value for " + key + " environment variable")
	}

	return int(result), true
}

func getDuration(key string) (time.Duration, bool) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return 0, false
	}

	result, err := time.ParseDuration(value)
	if err != nil {
		panic("expect duration value for " + key + " environment variable")
	}

	return result, true
}

func getAddr() string {
	addr, ok := getEnvString("BUS_ADDR")
	if !ok {
		return "0.0.0.0:2021"
	}

	return addr
}

func getLogLevel() slog.Level {
	level, ok := getEnvString("BUS_LOG_LEVEL")
	if !ok {
		return slog.LevelInfo
	}

	var logLevel slog.Level
	err := logLevel.UnmarshalText([]byte(level))
	if err != nil {
		panic("failed to parse log level:" + err.Error())
	}

	return logLevel
}

func storagePoolSize(opts []server.Opt) []server.Opt {
	value, ok := getEnvInt("BUS_STORAGE_POOL_SIZE")
	if !ok {
		return opts
	}

	return append(opts, server.WithDBPoolSize(value))
}

func getStoragePath(opts []server.Opt) []server.Opt {
	value, ok := getEnvString("BUS_STORAGE_PATH")
	if !ok {
		return opts
	}

	return append(opts, server.WithDBPath(value))
}

func getConsumerQueueSize(opts []server.Opt) []server.Opt {
	value, ok := getEnvInt("BUS_CONSUMER_QUEUE_SIZE")
	if !ok {
		return opts
	}
	return append(opts, server.WithConsumerQueueSize(value))
}

func getIncomingEventsBufferSize(opts []server.Opt) []server.Opt {
	value, ok := getEnvInt("BUS_INCOMING_EVENTS_BUFFER_SIZE")
	if !ok {
		return opts
	}
	return append(opts, server.WithIncomingEventsBufferSize(value))
}

func getTickTimeout(opts []server.Opt) []server.Opt {
	timeout, ok := getDuration("BUS_ACK_TICK_TIMEOUT")
	if !ok {
		return opts
	}

	size, ok := getEnvInt("BUS_ACK_TICK_SIZE")
	if !ok {
		return opts
	}

	return append(opts, server.WithAckTick(timeout, size))
}

func getEventsDeleteInterval(opts []server.Opt) []server.Opt {
	interval, ok := getDuration("BUS_EVENTS_DELETE_INTERVAL")
	if !ok {
		return opts
	}

	return append(opts, server.WithEventsDeleteInterval(interval))
}

func main() {
	ctx := context.Background()

	logLevel := getLogLevel()
	addr := getAddr()

	slog.SetLogLoggerLevel(logLevel)

	var opts []server.Opt

	opts = storagePoolSize(opts)
	opts = getStoragePath(opts)
	opts = getConsumerQueueSize(opts)
	opts = getIncomingEventsBufferSize(opts)
	opts = getTickTimeout(opts)
	opts = getEventsDeleteInterval(opts)

	handler, err := server.New(ctx, opts...)
	if err != nil {
		slog.Error("failed to initilaize server", "error", err.Error())
		os.Exit(1)
	}

	fmt.Printf(`
Version: %s
GitCommit: %s
	`, Version, GitCommit)

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
