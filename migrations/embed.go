// Package migrations exposes Saruman's embedded PostgreSQL migrations.
package migrations

import "embed"

// Files contains every paired up/down SQL migration.
//
//go:embed *.sql
var Files embed.FS
