package bus_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"ella.to/bus/client"
	"ella.to/bus/server"
	"github.com/stretchr/testify/assert"
)

func setupBusServer(t *testing.T) *client.Client {
	handler, err := server.New(
		context.TODO(),

		server.WithStoragePoolSize(10),
	)
	assert.NoError(t, err)

	server := httptest.NewServer(handler)
	// t.Cleanup(server.Close)

	c, err := client.New(client.WithAddr(server.URL))
	assert.NoError(t, err)

	return c
}
