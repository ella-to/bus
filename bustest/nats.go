package bustest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
)

func setRoot() error {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Dir(filename)

	for {
		goModFile := path.Join(dir, "go.mod")
		if _, err := os.Stat(goModFile); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		dir = path.Join(dir, "..")
	}

	err := os.Chdir(dir)
	if err != nil {
		return err
	}

	return nil
}

func StartNatsServer(t *testing.T, optFns ...bus.OptionsNatsFunc) string {
	server, err := bus.NewNatsServer(
		optFns...,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		slog.Debug("Closing test nats server")
		defer slog.Debug("Closed test nats server")
		server.Shutdown()
	})

	return server.ClientURL()
}

func StartDefaultNatsServer(t *testing.T, clientsCount int) string {
	err := setRoot()
	if err != nil {
		t.Fatal(err)
	}

	return StartNatsServer(
		t,
		bus.WithNatsTimeout(10*time.Second),
		bus.WithNatsConfigPath("bustest/testdata/nats/js.conf"),
		bus.WithNatsStoragePath(t.TempDir()),
		bus.WithNatsAutoPort(4222, 5222),
	)
}

func GetNatsClient(t *testing.T) *bus.NatsClient {
	ctx := context.Background()

	url := StartDefaultNatsServer(t, 1)

	client, err := bus.NewNatsClient(
		ctx,
		bus.WithNatsURL(url),
		bus.WithNatsStream("test", "test.>"),
	)
	assert.NoError(t, err)

	t.Cleanup(func() {
		slog.Debug("Closing test nats client")
		client.Close()
	})

	return client
}
