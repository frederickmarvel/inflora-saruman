package migrate_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/migrate"
	"github.com/frederickmarvel/inflora-saruman/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestBackofficeFeesNightlyMigrationRoundTrip covers 00017 (D-060/D-062/
// D-064): the streamer_settings.platform_fee_bps growth, the nightly-batch
// partial unique index, and the four backoffice permission rows with their
// ADMIN/SUPERADMIN grants — applied, rolled back one step, and re-applied
// from the very same files (checksum-stamped; nothing is renumbered). It
// follows the prepare/reset idiom of the sibling integration tests exactly:
// DROP SCHEMA public CASCADE, runner Up 0, assert, runner Down 16 (down TO
// version 16 = remove 00017 only), assert absent, runner Up 0, assert again.
func TestBackofficeFeesNightlyMigrationRoundTrip(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err = database.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	runner := migrate.Runner{DB: database, FS: migrations.Files, Environment: "test", AllowDown: true, IncludeSeed: true, LockTimeout: 5 * time.Second}

	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("up all: %v", err)
	}
	assertBackofficePresent(t, database)

	// Row-level: the CHECK bounds platform_fee_bps to 0..9999 (9999 cap, not
	// 10000 — saruman processor.go's frozen capture invariant net > 0).
	var sid string
	if err = database.QueryRowContext(ctx, `INSERT INTO streamers (email, display_name, password_hash, handle) VALUES ('backoffice-00017@example.com','Backoffice','x','bo-00017') RETURNING id`).Scan(&sid); err != nil {
		t.Fatalf("seed streamer for fee bounds: %v", err)
	}
	if _, err = database.ExecContext(ctx, `INSERT INTO streamer_settings (streamer_id, platform_fee_bps) VALUES ($1, 9999)`, sid); err != nil {
		t.Fatalf("insert max fee: %v", err)
	}
	if _, err = database.ExecContext(ctx, `INSERT INTO streamer_settings (streamer_id, platform_fee_bps) VALUES ($1, 10000)`, sid); err == nil {
		t.Fatal("expected 10000 bps to violate chk_ss_platform_fee_bps")
	}

	// Down one step (down TO version 16): only 00017 is removed.
	if err = runner.Run(ctx, migrate.Down, 16); err != nil {
		t.Fatalf("down 00017: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations`, 16)
	assertBackofficeAbsent(t, database)

	// Re-up through 00017 from the same files; idempotent and checksum-stamped.
	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("re-up 00017: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations WHERE version=17 AND name='backoffice_fees_nightly'`, 1)
	assertBackofficePresent(t, database)
}

// assertBackofficePresent asserts the full 00017 surface exists.
func assertBackofficePresent(t *testing.T, database *sql.DB) {
	t.Helper()
	// streamer_settings.platform_fee_bps: NOT NULL INTEGER DEFAULT 0 with
	// the 0..9999 CHECK.
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='streamer_settings' AND column_name='platform_fee_bps' AND data_type='integer' AND is_nullable='NO' AND column_default='0'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_constraint WHERE conname='chk_ss_platform_fee_bps'`, 1)
	// The nightly-batch guard: unique partial index on metadata->>'run_date'
	// where metadata->>'batch_kind'='nightly'.
	assertCount(t, database, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' AND indexname='uq_payout_batches_nightly_run'`, 1)
	// The four backoffice permission rows and their ADMIN/SUPERADMIN grants.
	assertCount(t, database, `SELECT COUNT(*) FROM permissions WHERE name IN ('Streamer:Read','Streamer:Write','MdrRate:Read','MdrRate:Write')`, 4)
	assertCount(t, database, `SELECT COUNT(*) FROM role_permissions rp JOIN roles r ON r.id=rp.role_id JOIN permissions p ON p.id=rp.permission_id WHERE r.name IN ('ADMIN','SUPERADMIN') AND p.name IN ('Streamer:Read','Streamer:Write','MdrRate:Read','MdrRate:Write')`, 8)
}

// assertBackofficeAbsent asserts every 00017 artifact is gone.
func assertBackofficeAbsent(t *testing.T, database *sql.DB) {
	t.Helper()
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='streamer_settings' AND column_name='platform_fee_bps'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_constraint WHERE conname='chk_ss_platform_fee_bps'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' AND indexname='uq_payout_batches_nightly_run'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM permissions WHERE name IN ('Streamer:Read','Streamer:Write','MdrRate:Read','MdrRate:Write')`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM role_permissions rp JOIN roles r ON r.id=rp.role_id JOIN permissions p ON p.id=rp.permission_id WHERE p.name IN ('Streamer:Read','Streamer:Write','MdrRate:Read','MdrRate:Write')`, 0)
}