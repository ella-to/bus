package storage

import (
	"embed"
)

//go:embed schema/*.sql
var MigrationFiles embed.FS
