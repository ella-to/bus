package testutil

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"ella.to/bus/client"
	"ella.to/bus/server"
	"ella.to/bus/storage"
)

type options struct {
	databasePath string
}

type optionFn func(*options)

func WithDatabasePath(path string) optionFn {
	return func(o *options) {
		o.databasePath = path
	}
}

func PrepareTestServer(t *testing.T, optFns ...optionFn) *client.Client {
	t.Helper()

	ctx := context.Background()

	opts := options{
		databasePath: "",
	}

	for _, fn := range optFns {
		fn(&opts)
	}

	storage, err := storage.NewSqlite(ctx, opts.databasePath)
	assert.NoError(t, err)

	server := server.New(ctx, storage)
	t.Cleanup(func() {
		server.Close()
	})

	return client.New(httptest.NewServer(server).URL)
}
