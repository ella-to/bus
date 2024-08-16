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

func PrepareTestServer(t *testing.T) *client.Client {
	t.Helper()

	ctx := context.Background()

	storage, err := storage.NewSqlite(ctx, "")
	assert.NoError(t, err)

	server := server.New(ctx, storage)
	t.Cleanup(func() {
		server.Close()
	})

	return client.New(httptest.NewServer(server).URL)
}
