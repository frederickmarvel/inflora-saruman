// Package migrate applies paired, embedded PostgreSQL migrations safely.
package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const advisoryLockKey int64 = 0x494E464C4F5241

var migrationName = regexp.MustCompile(`^(\d{5})_([a-z0-9_]+)\.(up|down)\.sql$`)

// Direction selects the migration operation.
type Direction string

// Supported migration directions.
const (
	Up     Direction = "up"
	Down   Direction = "down"
	Status Direction = "status"
)

// Runner applies migrations to a PostgreSQL database.
type Runner struct {
	DB          *sql.DB
	FS          embed.FS
	Environment string
	AllowDown   bool
	LockTimeout time.Duration
}

// Migration is a validated up/down SQL migration pair.
type Migration struct {
	Version  int64
	Name     string
	Up       string
	Down     string
	Checksum string
}

// Run executes the requested direction through target. A zero target means all.
func (r Runner) Run(ctx context.Context, direction Direction, target int64) error {
	if r.DB == nil {
		return errors.New("migrate: nil database")
	}
	if direction != Up && direction != Down && direction != Status {
		return fmt.Errorf("migrate: unsupported direction %q", direction)
	}
	if direction == Down && !r.AllowDown && !strings.EqualFold(r.Environment, "dev") && !strings.EqualFold(r.Environment, "test") {
		return errors.New("migrate: down migrations require ALLOW_DOWN_MIGRATIONS=true or ENV=dev/test")
	}
	ms, err := load(r.FS)
	if err != nil {
		return err
	}
	conn, err := r.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if r.LockTimeout > 0 {
		if _, err = conn.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, false)`, fmt.Sprintf("%dms", r.LockTimeout.Milliseconds())); err != nil {
			return fmt.Errorf("migrate: set lock timeout: %w", err)
		}
	}
	if _, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("migrate: acquire lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()
	if _, err = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum CHAR(64) NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		return fmt.Errorf("migrate: create metadata table: %w", err)
	}
	if direction == Status {
		return status(ctx, conn, ms)
	}
	if direction == Up {
		for _, m := range ms {
			if target > 0 && m.Version > target {
				break
			}
			if m.Name == "seed_dev" && !strings.EqualFold(r.Environment, "dev") && !strings.EqualFold(r.Environment, "test") {
				return errors.New("migrate: development seed requires ENV=dev or ENV=test")
			}
			applied, err := applied(ctx, conn, m)
			if err != nil {
				return err
			}
			if applied {
				continue
			}
			if err := apply(ctx, conn, m); err != nil {
				return err
			}
		}
		return nil
	}
	for i := len(ms) - 1; i >= 0; i-- {
		m := ms[i]
		if target > 0 && m.Version <= target {
			break
		}
		var exists bool
		if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version).Scan(&exists); err != nil {
			return fmt.Errorf("migrate: check %05d: %w", m.Version, err)
		}
		if !exists {
			continue
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("migrate: begin down %05d: %w", m.Version, err)
		}
		if _, err = tx.ExecContext(ctx, m.Down); err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.Version)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migrate: down %05d: %w", m.Version, err)
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("migrate: commit down %05d: %w", m.Version, err)
		}
	}
	return nil
}

func load(files embed.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("migrate: read embedded files: %w", err)
	}
	ups := map[int64]Migration{}
	downs := map[int64]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		match := migrationName.FindStringSubmatch(e.Name())
		if match == nil {
			continue
		}
		version, _ := strconv.ParseInt(match[1], 10, 64)
		body, err := files.ReadFile(filepath.ToSlash(e.Name()))
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", e.Name(), err)
		}
		if match[3] == "up" {
			if _, ok := ups[version]; ok {
				return nil, fmt.Errorf("migrate: duplicate version %d", version)
			}
			ups[version] = Migration{Version: version, Name: match[2], Up: string(body), Checksum: fmt.Sprintf("%x", sha256.Sum256(body))}
		} else {
			downs[version] = string(body)
		}
	}
	result := make([]Migration, 0, len(ups))
	for version, m := range ups {
		down, ok := downs[version]
		if !ok {
			return nil, fmt.Errorf("migrate: missing down migration for %05d", version)
		}
		m.Down = down
		result = append(result, m)
	}
	for version := range downs {
		if _, ok := ups[version]; !ok {
			return nil, fmt.Errorf("migrate: missing up migration for %05d", version)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })
	return result, nil
}

func applied(ctx context.Context, conn *sql.Conn, m Migration) (bool, error) {
	var checksum string
	err := conn.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, m.Version).Scan(&checksum)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("migrate: check %05d: %w", m.Version, err)
	}
	if checksum != m.Checksum {
		return false, fmt.Errorf("migrate: checksum mismatch for %05d", m.Version)
	}
	return true, nil
}

func apply(ctx context.Context, conn *sql.Conn, m Migration) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: begin %05d: %w", m.Version, err)
	}
	if _, err = tx.ExecContext(ctx, m.Up); err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`, m.Version, m.Name, m.Checksum)
	}
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("migrate: up %05d: %w", m.Version, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("migrate: commit %05d: %w", m.Version, err)
	}
	return nil
}

func status(ctx context.Context, conn *sql.Conn, ms []Migration) error {
	for _, m := range ms {
		var appliedAt time.Time
		err := conn.QueryRowContext(ctx, `SELECT applied_at FROM schema_migrations WHERE version = $1`, m.Version).Scan(&appliedAt)
		if errors.Is(err, sql.ErrNoRows) {
			fmt.Printf("%05d %-30s pending\n", m.Version, m.Name)
			continue
		}
		if err != nil {
			return err
		}
		fmt.Printf("%05d %-30s applied %s\n", m.Version, m.Name, appliedAt.UTC().Format(time.RFC3339))
	}
	return nil
}
