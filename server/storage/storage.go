package storage

import (
	"context"

	"ella.to/sqlite"
)

func New(ctx context.Context, opts ...sqlite.OptionFunc) (*sqlite.Database, error) {
	db, err := sqlite.New(opts...)
	if err != nil {
		return nil, err
	}

	err = sqlite.Migration(ctx, db, MigrationFiles, "schema")
	if err != nil {
		return nil, err
	}

	return db, nil
}
