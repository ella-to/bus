package db

import (
	"embed"
)

//go:embed schema/*.sql
var migrationFiles embed.FS
