package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"ella.to/bus/server/storage"
	"ella.to/sqlite"
)

func createTestStorage(t *testing.T, path string) *sqlite.Database {
	t.Helper()

	ops := []sqlite.OptionFunc{}

	if path != "" {
		ops = append(ops, sqlite.WithFile(path))
	} else {
		ops = append(ops, sqlite.WithMemory())
	}

	db, err := storage.New(context.TODO(), ops...)
	assert.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return db
}
