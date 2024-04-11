package db

import (
	"context"
	"embed"
	"log/slog"
	"path/filepath"
	"sort"

	"ella.to/bus.go/internal/sqlite"
)

//go:embed schema/*.sql
var migrationFiles embed.FS

// migration calls read each sql files in the migration directory and applies it to the database.
func migration(ctx context.Context, db *sqlite.Database) error {
	var sqlFiles []string

	files, err := migrationFiles.ReadDir("schema")
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".sql" {
			continue
		}

		sqlFiles = append(sqlFiles, filepath.Join("schema", file.Name()))
	}

	sort.Strings(sqlFiles)

	for _, sqlFile := range sqlFiles {
		slog.Debug("running migration sql", "file", sqlFile)

		content, err := migrationFiles.ReadFile(sqlFile)
		if err != nil {
			return err
		}

		err = sqlite.RunScript(ctx, db, string(content))
		if err != nil {
			return err
		}
	}

	return nil
}
