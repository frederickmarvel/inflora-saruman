package migrate_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/migrate"
	"github.com/frederickmarvel/inflora-saruman/migrations"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestPostgresMigrationPhase1UpDownReUp verifies Phase 1 schema can be applied,
// rolled back, and re-applied cleanly in an isolated database.
func TestPostgresMigrationPhase1UpDownReUp(t *testing.T) {
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
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'donations' AND column_name IN ('display_rate_idr_per_sec', 'display_min_sec', 'display_max_sec')`, 3)
	assertCount(t, database, `SELECT COUNT(*) FROM donations WHERE display_rate_idr_per_sec IS NULL AND display_min_sec IS NULL AND display_max_sec IS NULL`, 2)
	if _, err = database.ExecContext(ctx, `
		UPDATE donations
		SET display_rate_idr_per_sec = 1000, display_min_sec = 5, display_max_sec = 60, display_duration_sec = 10
		WHERE id = (SELECT id FROM donations ORDER BY id LIMIT 1)
	`); err != nil {
		t.Fatalf("write complete display-pricing snapshot: %v", err)
	}
	if _, err = database.ExecContext(ctx, `
		UPDATE donations
		SET display_rate_idr_per_sec = 1000, display_min_sec = NULL, display_max_sec = 60
		WHERE id = (SELECT id FROM donations ORDER BY id DESC LIMIT 1)
	`); err == nil {
		t.Fatal("expected partial display-pricing snapshot constraint violation")
	}
	if _, err = database.ExecContext(ctx, `
		UPDATE donations
		SET display_rate_idr_per_sec = 1000, display_min_sec = 5, display_max_sec = 60, display_duration_sec = 61
		WHERE id = (SELECT id FROM donations ORDER BY id DESC LIMIT 1)
	`); err == nil {
		t.Fatal("expected out-of-range display duration constraint violation")
	}
	if err = runner.Run(ctx, migrate.Down, 5); err != nil {
		t.Fatalf("down 00006: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'donations' AND column_name IN ('display_rate_idr_per_sec', 'display_min_sec', 'display_max_sec')`, 0)
	if err = runner.Run(ctx, migrate.Up, 6); err != nil {
		t.Fatalf("re-up 00006: %v", err)
	}
	// Down through every migration applied; we expect a clean schema.
	if err = runner.Run(ctx, migrate.Down, 0); err != nil {
		t.Fatalf("down all: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`, 0)

	// Re-up: must succeed idempotently.
	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	// After applying all migrations.
	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations`, 15)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_constraint WHERE conname = 'chk_idempotency_state'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_idempotency_in_progress'`, 1)
}

// TestRuntimeIdempotencyMigration verifies legacy rows are classified and
// timestamped correctly, invalid lifecycle values are rejected, and 00005 can
// be rolled back and re-applied without affecting 00003's dead-letter column.
func TestRuntimeIdempotencyMigration(t *testing.T) {
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
	if err = runner.Run(ctx, migrate.Up, 4); err != nil {
		t.Fatalf("up through 00004: %v", err)
	}
	if _, err = database.ExecContext(ctx, `
		INSERT INTO idempotency (key, endpoint, request_hash, response_status, response_body, created_at, processed_at)
		VALUES
		  ('legacy-pending', 'POST /v1/donations', repeat('a', 64), NULL, NULL, '2024-01-01T00:00:00Z', NULL),
		  ('legacy-processed', 'POST /v1/donations', repeat('b', 64), NULL, NULL, '2024-01-02T00:00:00Z', '2024-01-03T00:00:00Z'),
		  ('legacy-response', 'POST /v1/donations', repeat('c', 64), 200, '{"ok":true}', '2024-01-04T00:00:00Z', NULL)
	`); err != nil {
		t.Fatalf("seed legacy idempotency rows: %v", err)
	}
	if err = runner.Run(ctx, migrate.Up, 5); err != nil {
		t.Fatalf("up 00005: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM idempotency WHERE key = 'legacy-pending' AND state = 'IN_PROGRESS' AND updated_at = created_at`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM idempotency WHERE key = 'legacy-processed' AND state = 'COMPLETED' AND updated_at = processed_at`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM idempotency WHERE key = 'legacy-response' AND state = 'COMPLETED' AND updated_at = created_at`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'idempotency' AND column_name IN ('state', 'updated_at') AND is_nullable = 'NO'`, 2)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'outbox_events' AND column_name = 'dead_lettered_at'`, 1)
	if _, err = database.ExecContext(ctx, `UPDATE idempotency SET state = 'INVALID' WHERE key = 'legacy-pending'`); err == nil {
		t.Fatal("expected idempotency state check violation")
	}

	if err = runner.Run(ctx, migrate.Down, 4); err != nil {
		t.Fatalf("down 00005: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'idempotency' AND column_name IN ('state', 'updated_at')`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'outbox_events' AND column_name = 'dead_lettered_at'`, 1)
	if err = runner.Run(ctx, migrate.Up, 5); err != nil {
		t.Fatalf("re-up 00005: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations WHERE version = 5 AND name = 'runtime_reliability'`, 1)
}

// TestOutboxEventIDUnique asserts the schema rejects duplicate event_ids.
func TestOutboxEventIDUnique(t *testing.T) {
	resetSchema(t)
	database := openTestDB(t)
	t.Cleanup(func() { _ = database.Close() })
	seedStreamer(t, database)

	eventID := uuid.New()
	insertOutbox(t, eventID, "palantir", "pg.gateway.topup.completed.v1", "donation", uuid.New(), nil)
	if err := insertOutboxExpectError(t, eventID); err == nil {
		t.Fatalf("expected unique violation on duplicate event_id")
	}
}

// TestInboxConcurrentInserts asserts the consumer_inbox primary key holds
// under concurrent inserts of the same (consumer_name, event_id).
func TestInboxConcurrentInserts(t *testing.T) {
	resetSchema(t)
	database := openTestDB(t)
	t.Cleanup(func() { _ = database.Close() })
	seedStreamer(t, database)

	eventID := uuid.New()
	consumer := "saruman.topup-completed"
	var wg sync.WaitGroup
	var inserted int64
	var dup int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tag, err := database.Exec(
				`INSERT INTO consumer_inbox (consumer_name, event_id, event_type) VALUES ($1, $2, $3) ON CONFLICT (consumer_name, event_id) DO NOTHING`,
				consumer, eventID, "pg.gateway.topup.completed.v1",
			)
			if err != nil {
				t.Errorf("insert: %v", err)
				return
			}
			if n, _ := tag.RowsAffected(); n == 1 {
				atomic.AddInt64(&inserted, 1)
			} else {
				atomic.AddInt64(&dup, 1)
			}
		}()
	}
	wg.Wait()

	assertCount(t, database, `SELECT COUNT(*) FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2`, 1, consumer, eventID)
	if inserted != 1 {
		t.Fatalf("inserted = %d, want 1; dup = %d", inserted, dup)
	}
}

// TestLedgerOperationKeyConcurrentUnique asserts ledger_transactions.operation_key
// holds under concurrent inserts of the same value.
func TestLedgerOperationKeyConcurrentUnique(t *testing.T) {
	resetSchema(t)
	database := openTestDB(t)
	t.Cleanup(func() { _ = database.Close() })
	streamerID := seedStreamer(t, database)

	opKey := fmt.Sprintf("donation_capture:%s", uuid.New())
	var wg sync.WaitGroup
	var conflicts int64
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := database.Exec(
				`INSERT INTO ledger_transactions (operation_key, transaction_type, streamer_id) VALUES ($1, $2, $3) ON CONFLICT (operation_key) DO NOTHING`,
				opKey, "DONATION_CAPTURE", streamerID,
			)
			if err != nil {
				atomic.AddInt64(&conflicts, 1)
			}
		}()
	}
	wg.Wait()
	assertCount(t, database, `SELECT COUNT(*) FROM ledger_transactions WHERE operation_key = $1`, 1, opKey)
}

// TestDeadLetterUpsertUnique asserts that (consumer_name, event_id) cannot be
// duplicated, so the dead-letter table acts as a hard durable guarantee.
func TestDeadLetterUpsertUnique(t *testing.T) {
	resetSchema(t)
	database := openTestDB(t)
	t.Cleanup(func() { _ = database.Close() })

	eventID := uuid.New()
	envelope := json.RawMessage(`{"hello":"world"}`)
	if _, err := database.Exec(
		`INSERT INTO event_dead_letters (consumer_name, event_id, subject, envelope, attempt_count, error_class, last_error, first_failed_at, last_failed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())`,
		"saruman.topup-completed", eventID, "pg.gateway.topup.completed.v1.x", envelope, 1, "PERMANENT", "boom",
	); err != nil {
		t.Fatalf("insert first: %v", err)
	}
	if _, err := database.Exec(
		`INSERT INTO event_dead_letters (consumer_name, event_id, subject, envelope, attempt_count, error_class, last_error, first_failed_at, last_failed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())`,
		"saruman.topup-completed", eventID, "pg.gateway.topup.completed.v1.x", envelope, 1, "PERMANENT", "boom",
	); err == nil {
		t.Fatalf("expected unique violation on duplicate dead letter")
	}
}

// TestOutboxSkipLocked asserts two concurrent claim transactions see disjoint
// rows: a critical property for the Phase 2 dispatcher.
func TestOutboxSkipLocked(t *testing.T) {
	resetSchema(t)
	database := openTestDB(t)
	t.Cleanup(func() { _ = database.Close() })
	seedStreamer(t, database)

	const totalRows = 50
	const batchSize = 10
	for i := 0; i < totalRows; i++ {
		insertOutbox(t, uuid.New(), "saruman", "donation.charged.v1", "donation", uuid.New(), nil)
	}

	tx1, err := database.Begin()
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback()
	rows1, err := tx1.Query(
		`SELECT event_id FROM outbox_events WHERE published_at IS NULL ORDER BY available_at ASC LIMIT $1 FOR UPDATE SKIP LOCKED`,
		batchSize,
	)
	if err != nil {
		t.Fatalf("claim tx1: %v", err)
	}
	var claimed1 []string
	for rows1.Next() {
		var id string
		if err := rows1.Scan(&id); err != nil {
			t.Fatalf("scan tx1: %v", err)
		}
		claimed1 = append(claimed1, id)
	}
	rows1.Close()
	if len(claimed1) != batchSize {
		t.Fatalf("tx1 claimed = %d, want %d", len(claimed1), batchSize)
	}

	tx2, err := database.Begin()
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback()
	rows2, err := tx2.Query(
		`SELECT event_id FROM outbox_events WHERE published_at IS NULL ORDER BY available_at ASC LIMIT $1 FOR UPDATE SKIP LOCKED`,
		batchSize,
	)
	if err != nil {
		t.Fatalf("claim tx2: %v", err)
	}
	var claimed2 []string
	for rows2.Next() {
		var id string
		if err := rows2.Scan(&id); err != nil {
			t.Fatalf("scan tx2: %v", err)
		}
		claimed2 = append(claimed2, id)
	}
	rows2.Close()
	if len(claimed2) != batchSize {
		t.Fatalf("tx2 claimed = %d, want %d", len(claimed2), batchSize)
	}

	if intersectAny(claimed1, claimed2) {
		t.Fatalf("SKIP LOCKED violated: shared event_id between tx1 and tx2")
	}
}

// TestWSSequenceAllocationConcurrentUnique asserts two concurrent allocations
// for the same streamer produce unique increasing seqs.
func TestWSSequenceAllocationConcurrentUnique(t *testing.T) {
	resetSchema(t)
	database := openTestDB(t)
	t.Cleanup(func() { _ = database.Close() })
	streamerID := seedStreamer(t, database)

	if _, err := database.Exec(`INSERT INTO ws_projection_sequences (streamer_id, next_seq) VALUES ($1, 1)`, streamerID); err != nil {
		t.Fatalf("seed sequence: %v", err)
	}

	const writers = 16
	const allocs = 50
	var wg sync.WaitGroup
	var seqs sync.Map
	var collisions int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < allocs; i++ {
				tx, err := database.Begin()
				if err != nil {
					atomic.AddInt64(&collisions, 1)
					continue
				}
				var seq int64
				if err := tx.QueryRow(
					`UPDATE ws_projection_sequences SET next_seq = next_seq + 1, updated_at = NOW() WHERE streamer_id = $1 RETURNING next_seq - 1`,
					streamerID,
				).Scan(&seq); err != nil {
					_ = tx.Rollback()
					atomic.AddInt64(&collisions, 1)
					continue
				}
				if _, err := tx.Exec(
					`INSERT INTO ws_projection_frames (streamer_id, seq, source_event_id, event_type, payload) VALUES ($1,$2,$3,$4,$5)`,
					streamerID, seq, uuid.New(), "donation.charged.v1", json.RawMessage(`{"x":1}`),
				); err != nil {
					_ = tx.Rollback()
					atomic.AddInt64(&collisions, 1)
					continue
				}
				if err := tx.Commit(); err != nil {
					atomic.AddInt64(&collisions, 1)
					continue
				}
				if _, dup := seqs.LoadOrStore(seq, struct{}{}); dup {
					atomic.AddInt64(&collisions, 1)
				}
			}
		}()
	}
	wg.Wait()
	if collisions > 0 {
		t.Fatalf("collisions = %d", collisions)
	}
	row := database.QueryRow(`SELECT next_seq - 1 FROM ws_projection_sequences WHERE streamer_id = $1`, streamerID)
	var maxSeq int64
	if err := row.Scan(&maxSeq); err != nil {
		t.Fatalf("scan max seq: %v", err)
	}
	if maxSeq != int64(writers*allocs) {
		t.Fatalf("maxSeq = %d, want %d", maxSeq, writers*allocs)
	}
}

// --- test setup helpers ---

func resetSchema(t *testing.T) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer database.Close()
	if _, err = database.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	runner := migrate.Runner{DB: database, FS: migrations.Files, Environment: "test", AllowDown: true, LockTimeout: 5 * time.Second}
	if err := runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("apply all: %v", err)
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return database
}

func seedStreamer(t *testing.T, db *sql.DB) string {
	t.Helper()
	id := uuid.New().String()
	if _, err := db.Exec(
		`INSERT INTO streamers (id, email, display_name, handle, password_hash) VALUES ($1, $2, $3, $4, $5)`,
		id, id+"@example.com", "Test Streamer", "test-"+id[:8], "$2a$12$placeholder",
	); err != nil {
		t.Fatalf("seed streamer: %v", err)
	}
	return id
}

func insertOutbox(t *testing.T, eventID uuid.UUID, producer, eventType, aggType string, aggID uuid.UUID, streamerID *string) {
	t.Helper()
	var streamer sql.NullString
	if streamerID != nil {
		streamer = sql.NullString{String: *streamerID, Valid: true}
	}
	envelope := json.RawMessage(`{"event_id":"` + eventID.String() + `"}`)
	if _, err := openTestDBHandle(t).Exec(
		`INSERT INTO outbox_events (event_id, producer, event_type, event_version, aggregate_type, aggregate_id, streamer_id, subject, envelope, occurred_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())`,
		eventID, producer, eventType, "1", aggType, aggID, streamer, "subj.x."+eventID.String(), envelope,
	); err != nil {
		t.Fatalf("insert outbox: %v", err)
	}
}

func insertOutboxExpectError(t *testing.T, eventID uuid.UUID) error {
	t.Helper()
	_, err := openTestDBHandle(t).Exec(
		`INSERT INTO outbox_events (event_id, producer, event_type, event_version, aggregate_type, aggregate_id, subject, envelope, occurred_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())`,
		eventID, "palantir", "pg.gateway.topup.completed.v1", "1", "donation", uuid.New(), "subj.x."+eventID.String(), json.RawMessage(`{}`),
	)
	return err
}

var dbHandle sync.Map

func openTestDBHandle(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func intersectAny(a, b []string) bool {
	idx := make(map[string]bool, len(a))
	for _, v := range a {
		idx[v] = true
	}
	for _, v := range b {
		if idx[v] {
			return true
		}
	}
	return false
}

// Compile-time guards.
var _ = errors.New

// TestSupporterIdentityMigration covers 00013 (D-035/D-044 A-3): the physical
// supporters bridge table, the donations identity columns, and — critically —
// that the frozen token_purpose enum is untouched (auth is deferred to Phase
// A; no supporter session scheme ships with this migration).
func TestSupporterIdentityMigration(t *testing.T) {
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

	// supporters: exactly the A-3 bridge shape — no password_hash (Firebase
	// auth deferred, owner ack 2026-10-18).
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='supporters'`, 8)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='supporters' AND column_name IN ('email','display_name','firebase_uid','is_active','last_login_at')`, 5)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='supporters' AND column_name='password_hash'`, 0)
	// donations identity columns + FK + partial index.
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='donations' AND column_name IN ('supporter_id','claimed_at')`, 2)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_constraint WHERE conname = 'donations_supporter_id_fkey' AND contype='f'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' AND indexname='idx_donations_supporter'`, 1)
	// Frozen-enum regression: token_purpose stays exactly (SESSION, OVERLAY).
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='token_purpose'`, 2)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='token_purpose' AND e.enumlabel IN ('SESSION','OVERLAY')`, 2)

	// Row-level: a supporter can be written and both NULL and set
	// supporter_id are accepted (guest tips stay anonymous).
	var sid string
	if err = database.QueryRowContext(ctx, `INSERT INTO supporters (email, display_name) VALUES ('donor@example.com','Donor') RETURNING id`).Scan(&sid); err != nil {
		t.Fatalf("insert supporter: %v", err)
	}
	if _, err = database.ExecContext(ctx, `UPDATE donations SET supporter_id=$1, claimed_at=NOW() WHERE id=(SELECT id FROM donations ORDER BY id LIMIT 1)`, sid); err != nil {
		t.Fatalf("claim oldest donation: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM donations WHERE supporter_id = $1`, 1, sid)
	assertCount(t, database, `SELECT COUNT(*) FROM donations WHERE supporter_id IS NULL`, 1) // second seed row stays anonymous

	// Down/up: columns and table disappear cleanly, then return.
	if err = runner.Run(ctx, migrate.Down, 12); err != nil {
		t.Fatalf("down 00013: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='supporters'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='donations' AND column_name IN ('supporter_id','claimed_at')`, 0)
	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='supporters'`, 1)
}

// TestPayoutWithdrawalsMigration verifies 00014's payout-robustness growth
// (D-047..D-058) and the 00016 dev seed: enum growth, claim/lease columns,
// batch custody, gateway mirror columns, the DISBURSED balance surfacing, and
// a clean down/re-up — including the enum rebuilds that must drop the new
// values' data rows first.
func TestPayoutWithdrawalsMigration(t *testing.T) {
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

	// Enum growth: payout_status gains NEEDS_REVIEW, ledger_account_type gains
	// DISBURSED, ledger_entry_type gains STREAMER_DISBURSED_CREDIT (D-048).
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='payout_status' AND e.enumlabel='NEEDS_REVIEW'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='ledger_account_type' AND e.enumlabel='DISBURSED'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='ledger_entry_type' AND e.enumlabel='STREAMER_DISBURSED_CREDIT'`, 1)
	// Payout claim/lease columns + physical batch FK (D-055/D-057).
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='payouts' AND column_name IN ('attempts','lease_expires_at','last_event_id')`, 3)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_constraint WHERE conname='fk_payouts_batch' AND contype='f'`, 1)
	// Batch custody columns (D-054).
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='payout_batches' AND column_name IN ('submitted_by','approved_by','paused','resolution_note')`, 4)
	// Gateway mirror columns + one-attempt-per-payout unique index.
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='gateway_withdrawals' AND column_name IN ('provider_name','provider_payout_id','bank_account_ref','status_checked_at','last_known_provider_status')`, 5)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' AND indexname='uq_gw_withdrawal_provider_payout'`, 1)
	// Recon deferral kind (D-058).
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='reconciliation_drift' AND column_name='kind'`, 1)
	// Balance surface gained the disbursed tracker (D-048).
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='v_streamer_balances' AND column_name='disbursed_idr'`, 1)

	// 00016 dev seed: verified PRIMARY bank rows for the two dev streamers.
	assertCount(t, database, `SELECT COUNT(*) FROM streamer_bank_accounts WHERE palantir_ref IN ('pivot-bank-dev-1','pivot-bank-dev-2') AND is_verified`, 2)

	// Row-level: the new payout state and account type accept rows; a provider
	// mirror row is rejected twice for the same payout (unique index).
	if _, err = database.ExecContext(ctx, `INSERT INTO payouts (streamer_id, amount_idr, bank_account_id) SELECT '00000000-0000-4000-8000-000000000001', 10000, id FROM streamer_bank_accounts WHERE palantir_ref='pivot-bank-dev-1'`); err != nil {
		t.Fatalf("insert payout: %v", err)
	}
	var payoutID string
	var bankRef string
	if err = database.QueryRowContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW' WHERE streamer_id='00000000-0000-4000-8000-000000000001' RETURNING id`).Scan(&payoutID); err != nil {
		t.Fatalf("needs-review payout row: %v", err)
	}
	if err = database.QueryRowContext(ctx, `SELECT palantir_ref FROM streamer_bank_accounts WHERE palantir_ref='pivot-bank-dev-1'`).Scan(&bankRef); err != nil {
		t.Fatalf("seed bank row: %v", err)
	}
	if _, err = database.ExecContext(ctx, `INSERT INTO ledger_accounts (streamer_id, account_type) VALUES ('00000000-0000-4000-8000-000000000001', 'DISBURSED')`); err != nil {
		t.Fatalf("insert disbursed account: %v", err)
	}
	if _, err = database.ExecContext(ctx, `INSERT INTO gateway_withdrawals (palantir_ref, saruman_payout_id, streamer_id, amount_idr, status, provider_name) VALUES ('gw-test-1', $1, '00000000-0000-4000-8000-000000000001', 10000, 'REQUESTED', 'pivot')`, payoutID); err != nil {
		t.Fatalf("insert gateway withdrawal: %v", err)
	}
	if _, err = database.ExecContext(ctx, `INSERT INTO gateway_withdrawals (palantir_ref, saruman_payout_id, streamer_id, amount_idr, status, provider_name) VALUES ('gw-test-2', $1, '00000000-0000-4000-8000-000000000001', 10000, 'REQUESTED', 'pivot')`, payoutID); err == nil {
		t.Fatal("expected duplicate provider attempt to violate the unique index")
	}

	// Down 00014: the rollback deletes rows holding the dropped enum values,
	// rebuilds both enums, and strips the disbursed toggle from the balance view.
	if err = runner.Run(ctx, migrate.Down, 13); err != nil {
		t.Fatalf("down 00014: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='payout_status'`, 5)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='ledger_account_type'`, 6)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='ledger_entry_type'`, 10)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='v_streamer_balances' AND column_name='disbursed_idr'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='payouts' AND column_name IN ('attempts','lease_expires_at','last_event_id')`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' AND indexname='uq_gw_withdrawal_provider_payout'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM ledger_accounts WHERE account_type::text = 'DISBURSED'`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM payouts WHERE status::text = 'NEEDS_REVIEW'`, 0)

	// Re-up: growth returns; the seeded bank rows come back.
	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='payout_status' AND e.enumlabel='NEEDS_REVIEW'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='v_streamer_balances' AND column_name='disbursed_idr'`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM streamer_bank_accounts WHERE palantir_ref IN ('pivot-bank-dev-1','pivot-bank-dev-2')`, 2)
}
