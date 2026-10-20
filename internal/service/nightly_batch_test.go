package service

// T27 — nightly withdrawal sweep (CONTRACT_DRIFT D-062) and batch metadata
// (D-054 + 00017). RunNightlyBatch is a real-Postgres posture: every guard is
// DB-side (marker row + partial unique index, deterministic payout ids,
// FOR UPDATE OF s SKIP LOCKED candidates), so the matrix runs against an
// isolated schema (nightly_batch_test) like the T26 concurrency test — never
// the shared public schema of inflora_test.
//
// Helper prefix: nb (unique to this file). The isolated-schema idiom
// (transaction-per-migration apply with public. -> nbSchema. rewriting, the
// second handle whose DSN pins search_path, single-connection setup handle)
// mirrors payout_concurrency_test.go deliberately; each helper is local.
//
// NOTE on two documented production-vs-contract drifts the tests pin as-is:
//   - a same-run_date re-run returns the existing marker batch id, not
//     uuid.Nil (nightly_batch.go marker check); NightlyBatcher.tick depends on
//     that branch, so the tests assert the marker id.
//   - CreateBatch claims its members, so a swept payout is BATCHED, not
//     REQUESTED, when RunNightlyBatch returns.

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-saruman/migrations"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const nbSchema = "nightly_batch_test"

// nbRunDate is the sweep date every test here runs for; each test gets a
// fresh schema, so the same date is safe across tests.
const nbRunDate = "2026-10-01"

// nbNow is the wall clock RunNightlyBatch formats nbRunDate from.
var nbNow = time.Date(2026, 10, 1, 0, 30, 0, 0, time.UTC)

// nbTestDB drops/recreates the isolated schema, applies every non-seed
// migration, and returns a single-connection handle with search_path pinned.
// A second handle (nbSecondConn) exists for the lock tests.
func nbTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("nb: open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS `+nbSchema+` CASCADE; CREATE SCHEMA `+nbSchema); err != nil {
		t.Fatalf("nb: create schema: %v", err)
	}
	// Isolated schema first, public second: the citext extension lives in
	// public and extensions are database-wide (see ccTestDB's comment).
	if _, err := db.ExecContext(ctx, `SET search_path TO `+nbSchema+`, public`); err != nil {
		t.Fatalf("nb: set search_path: %v", err)
	}
	nbApplyMigrations(t, db)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+nbSchema+` CASCADE`)
		_ = db.Close()
	})
	return db
}

// nbSecondConn opens a second pooled handle whose DSN pins search_path, so
// its connections see the isolated schema (the pgx runtime-parameter trick
// from ccTestDB).
func nbSecondConn(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	raceDSN := dsn
	if strings.Contains(raceDSN, "?") {
		raceDSN += "&search_path=" + nbSchema + ",public"
	} else {
		raceDSN += "?search_path=" + nbSchema + ",public"
	}
	db, err := sql.Open("pgx", raceDSN)
	if err != nil {
		t.Fatalf("nb: open second conn: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// nbApplyMigrations applies every non-seed up.sql from the embedded FS in
// version order, rewriting hardcoded public. references to the isolated
// schema — the same loop ccApplyMigrations runs for the T26 schema.
func nbApplyMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatalf("nb: read migration dir: %v", err)
	}
	type nbMigration struct {
		version int64
		name    string
		up      string
	}
	var ms []nbMigration
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		if len(name) < 6 || name[5] != '_' {
			t.Fatalf("nb: unexpected migration file %q", name)
		}
		version, err := strconv.ParseInt(name[:5], 10, 64)
		if err != nil {
			t.Fatalf("nb: parse migration version %q: %v", name, err)
		}
		rest := name[6 : len(name)-len(".up.sql")]
		if strings.HasPrefix(rest, "seed_") {
			continue // dev seeds are not schema and never apply here
		}
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatalf("nb: read migration %s: %v", name, err)
		}
		ms = append(ms, nbMigration{version: version, name: name, up: strings.ReplaceAll(string(body), "public.", nbSchema+".")})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	for _, m := range ms {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("nb: begin %s: %v", m.name, err)
		}
		if _, err = tx.Exec(m.up); err != nil {
			_ = tx.Rollback()
			t.Fatalf("nb: apply %s: %v", m.name, err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatalf("nb: commit %s: %v", m.name, err)
		}
	}
}

// nbSeedStreamer inserts an active streamer (verification flag optional on
// its bank account) with STREAMER_AVAILABLE funded to `available` IDR.
// CreatePayout's ensurePayoutAccounts backfills the other ledger accounts.
func nbSeedStreamer(t *testing.T, db *sql.DB, available int64, verifiedBank bool) (streamerID, bankID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	streamerID, bankID = uuid.New(), uuid.New()
	if _, err := db.ExecContext(ctx, `INSERT INTO streamers (id,email,display_name,password_hash,handle) VALUES ($1,$2,$3,$4,$5)`,
		streamerID, "nb-"+streamerID.String()+"@example.com", "nb-streamer", "x", "nb-"+streamerID.String()[:8]); err != nil {
		t.Fatalf("nb: seed streamer: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO streamer_bank_accounts (id,streamer_id,bank_code,account_number_last4,account_name,is_primary,is_verified,palantir_ref) VALUES ($1,$2,'BCA','1234','NB Streamer',TRUE,$3,$4)`,
		bankID, streamerID, verifiedBank, "nb-palantir-"+streamerID.String()); err != nil {
		t.Fatalf("nb: seed bank account: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO ledger_accounts (streamer_id,account_type,currency,balance_idr) VALUES ($1,'STREAMER_AVAILABLE','IDR',$2)`, streamerID, available); err != nil {
		t.Fatalf("nb: seed available account: %v", err)
	}
	return streamerID, bankID
}

// nbDerivedPayoutID is the READ-side assertion of the deterministic D-062 id:
// the idempotency key "nightly-sweep:<run_date>" hashed against the streamer.
func nbDerivedPayoutID(streamerID string) string {
	return keys.PayoutRequestIDKey(streamerID, "nightly-sweep:"+nbRunDate)
}

// nbAddFundHold places an ACTIVE STREAMER/PAYOUT-targeted hold. A PAYOUT hold
// needs a payout row first (fund_holds CHECK target consistency).
func nbAddFundHold(t *testing.T, db *sql.DB, actorAdminID, streamerID, bankID uuid.UUID, target string) {
	t.Helper()
	ctx := context.Background()
	var err error
	if target == "PAYOUT" {
		payoutID := uuid.New()
		if _, err = db.ExecContext(ctx, `INSERT INTO payouts (id,streamer_id,amount_idr,bank_account_id) VALUES ($1,$2,$3,$4)`, payoutID, streamerID, int64(10000), bankID); err != nil {
			t.Fatalf("nb: seed hold target payout: %v", err)
		}
		_, err = db.ExecContext(ctx, `INSERT INTO fund_holds (streamer_id,payout_id,target_type,reason,created_by) VALUES ($1,$2,'PAYOUT','nb test hold',$3)`, streamerID, payoutID, actorAdminID)
	} else {
		_, err = db.ExecContext(ctx, `INSERT INTO fund_holds (streamer_id,target_type,reason,created_by) VALUES ($1,'STREAMER','nb test hold',$2)`, streamerID, actorAdminID)
	}
	if err != nil {
		t.Fatalf("nb: seed %s hold: %v", target, err)
	}
}

// nbMustStr / nbMustInt64 are single-value query helpers.
func nbMustStr(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var v string
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("nb: %s: %v", query, err)
	}
	return v
}

func nbMustInt64(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("nb: %s: %v", query, err)
	}
	return v
}

// --- RunNightlyBatch matrix -------------------------------------------------

// A pre-existing nightly batch row for the same run_date makes the run a
// no-op BEFORE any candidate is considered: even with an eligible streamer
// seeded, nothing is created and the existing batch id comes back (production
// returns the marker id — see the file-header note — created stays 0).
func TestNbRunNightlyBatchPreExistingMarkerIsNoOp(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()
	_, _ = nbSeedStreamer(t, db, 25000, true)

	markerID := uuid.New()
	if _, err := db.ExecContext(ctx, `INSERT INTO payout_batches (id,created_by,metadata) VALUES ($1,$2,'{"batch_kind":"nightly","run_date":"`+nbRunDate+`"}'::jsonb)`, markerID, actorAdminID); err != nil {
		t.Fatalf("nb: seed marker batch: %v", err)
	}

	created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil {
		t.Fatalf("nb: run err = %v", err)
	}
	if created != 0 {
		t.Fatalf("nb: created = %d, want 0", created)
	}
	if batchID != markerID {
		t.Fatalf("nb: batchID = %s, want the pre-existing marker %s", batchID, markerID)
	}
	if len(skipped) != 0 {
		t.Fatalf("nb: skipped = %v, want none", skipped)
	}
	// The eligible streamer was never touched: no payout, no second batch.
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts`); got != 0 {
		t.Fatalf("nb: payout rows = %d, want 0 (no candidates queried)", got)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 1 {
		t.Fatalf("nb: batch rows = %d, want just the marker", got)
	}
	// Re-run: same no-op (and the same marker id).
	created, batchID2, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil || created != 0 || batchID2 != markerID || len(skipped) != 0 {
		t.Fatalf("nb: re-run = (%d, %s, %v, %v), want (0, %s, [], nil)", created, batchID2, skipped, err, markerID)
	}
}

// One eligible streamer: exactly one payout for the full available balance
// under the deterministic id, DRAFT batch with the nightly metadata and the
// actor as creator. The member was claimed by CreateBatch, so its status is
// BATCHED and its batch_id links the batch.
func TestNbRunNightlyBatchSingleEligibleStreamer(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()
	streamerID, bankID := nbSeedStreamer(t, db, 25000, true)

	created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil {
		t.Fatalf("nb: run err = %v", err)
	}
	if created != 1 {
		t.Fatalf("nb: created = %d, want 1", created)
	}
	if batchID == uuid.Nil {
		t.Fatal("nb: batchID is nil")
	}
	if len(skipped) != 0 {
		t.Fatalf("nb: skipped = %v, want none", skipped)
	}

	derived := nbDerivedPayoutID(streamerID.String())
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts`); got != 1 {
		t.Fatalf("nb: payout rows = %d, want 1", got)
	}
	if got := nbMustStr(t, db, `SELECT id::text FROM payouts`); got != derived {
		t.Fatalf("nb: payout id = %s, want derived %s", got, derived)
	}
	if got := nbMustInt64(t, db, `SELECT amount_idr FROM payouts WHERE id=$1`, derived); got != 25000 {
		t.Fatalf("nb: payout amount = %d, want 25000 exactly", got)
	}
	if got := nbMustStr(t, db, `SELECT bank_account_id::text FROM payouts WHERE id=$1`, derived); got != bankID.String() {
		t.Fatalf("nb: payout bank = %s, want %s", got, bankID)
	}
	if got := nbMustStr(t, db, `SELECT status::text FROM payouts WHERE id=$1`, derived); got != PayoutBatched {
		t.Fatalf("nb: payout status = %q, want %q (CreateBatch claims members)", got, PayoutBatched)
	}
	if got := nbMustStr(t, db, `SELECT batch_id::text FROM payouts WHERE id=$1`, derived); got != batchID.String() {
		t.Fatalf("nb: payout batch_id = %s, want %s", got, batchID)
	}

	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 1 {
		t.Fatalf("nb: batch rows = %d, want 1", got)
	}
	if got := nbMustStr(t, db, `SELECT status::text FROM payout_batches WHERE id=$1`, batchID); got != BatchDraft {
		t.Fatalf("nb: batch status = %q, want %q", got, BatchDraft)
	}
	if got := nbMustStr(t, db, `SELECT created_by::text FROM payout_batches WHERE id=$1`, batchID); got != actorAdminID.String() {
		t.Fatalf("nb: batch created_by = %s, want %s", got, actorAdminID)
	}
	if got := nbMustInt64(t, db, `SELECT payout_count FROM payout_batches WHERE id=$1`, batchID); got != 1 {
		t.Fatalf("nb: batch payout_count = %d, want 1", got)
	}
	if got := nbMustInt64(t, db, `SELECT total_amount_idr FROM payout_batches WHERE id=$1`, batchID); got != 25000 {
		t.Fatalf("nb: batch total = %d, want 25000", got)
	}
	if got := nbMustStr(t, db, `SELECT metadata->>'batch_kind' FROM payout_batches WHERE id=$1`, batchID); got != "nightly" {
		t.Fatalf("nb: batch_kind = %q, want nightly", got)
	}
	if got := nbMustStr(t, db, `SELECT metadata->>'run_date' FROM payout_batches WHERE id=$1`, batchID); got != nbRunDate {
		t.Fatalf("nb: run_date = %q, want %q", got, nbRunDate)
	}
	// The s3 reserve moved the full balance out of available.
	if got := nbMustInt64(t, db, `SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type='STREAMER_AVAILABLE'`, streamerID); got != 0 {
		t.Fatalf("nb: available after sweep = %d, want 0", got)
	}
	// Same-date re-run: the fresh batch row is the marker, nothing re-swept.
	reCreated, reBatchID, reSkipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil || reCreated != 0 || reBatchID != batchID || len(reSkipped) != 0 {
		t.Fatalf("nb: re-run = (%d, %s, %v, %v), want (0, %s, [], nil)", reCreated, reBatchID, reSkipped, err, batchID)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts`); got != 1 {
		t.Fatalf("nb: payout rows after re-run = %d, want 1", got)
	}
}

// The four ineligible shapes never even reach CreatePayout — the candidates
// query excludes them (balance < MinPayoutIDR, no verified bank, ACTIVE
// STREAMER/PAYOUT hold) — so skipped stays empty, nothing is created, and no
// batch row exists. The deterministic payout id is absent for each.
func TestNbRunNightlyBatchSkipsIneligibleStreamers(t *testing.T) {
	tests := []struct {
		name    string
		seed    func(t *testing.T, db *sql.DB, admin uuid.UUID) (streamerID, bankID uuid.UUID)
	}{
		{"under minimum balance", func(t *testing.T, db *sql.DB, a uuid.UUID) (uuid.UUID, uuid.UUID) {
			s, b := nbSeedStreamer(t, db, 9999, true)
			return s, b
		}},
		{"no verified bank", func(t *testing.T, db *sql.DB, a uuid.UUID) (uuid.UUID, uuid.UUID) {
			s, b := nbSeedStreamer(t, db, 25000, false)
			return s, b
		}},
		{"active streamer hold", func(t *testing.T, db *sql.DB, a uuid.UUID) (uuid.UUID, uuid.UUID) {
			s, b := nbSeedStreamer(t, db, 25000, true)
			nbAddFundHold(t, db, a, s, b, "STREAMER")
			return s, b
		}},
		{"active payout hold", func(t *testing.T, db *sql.DB, a uuid.UUID) (uuid.UUID, uuid.UUID) {
			s, b := nbSeedStreamer(t, db, 25000, true)
			nbAddFundHold(t, db, a, s, b, "PAYOUT")
			return s, b
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := nbTestDB(t)
			ctx := context.Background()
			actorAdminID := uuid.New()
			streamerID, _ := tt.seed(t, db, actorAdminID)

			created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
			if err != nil {
				t.Fatalf("nb: run err = %v", err)
			}
			if created != 0 {
				t.Fatalf("nb: created = %d, want 0", created)
			}
			if batchID != uuid.Nil {
				t.Fatalf("nb: batchID = %s, want nil", batchID)
			}
			if len(skipped) != 0 {
				t.Fatalf("nb: skipped = %v, want empty (the candidates query excludes these, so no per-candidate reason is produced)", skipped)
			}
			if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts WHERE streamer_id=$1 AND id=$2`, streamerID, nbDerivedPayoutID(streamerID.String())); got != 0 {
				t.Fatalf("nb: derived payout exists, want none")
			}
			if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 0 {
				t.Fatalf("nb: batch rows = %d, want 0", got)
			}
		})
	}
}

// A payout already carrying the derived id (a prior partial run) is skipped
// WITHOUT recomputing the amount: the seeded 12345 survives the 25000
// available untouched, no second payout appears, no batch is created, and the
// streamer is reported in skipped.
func TestNbRunNightlyBatchSkipsExistingDerivedPayout(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()
	streamerID, bankID := nbSeedStreamer(t, db, 25000, true)

	derived := nbDerivedPayoutID(streamerID.String())
	if _, err := db.ExecContext(ctx, `INSERT INTO payouts (id,streamer_id,amount_idr,bank_account_id) VALUES ($1,$2,$3,$4)`,
		derived, streamerID, int64(12345), bankID); err != nil {
		t.Fatalf("nb: pre-create derived payout: %v", err)
	}

	created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil {
		t.Fatalf("nb: run err = %v", err)
	}
	if created != 0 {
		t.Fatalf("nb: created = %d, want 0", created)
	}
	if batchID != uuid.Nil {
		t.Fatalf("nb: batchID = %s, want nil", batchID)
	}
	if len(skipped) != 1 || skipped[0] != streamerID.String() {
		t.Fatalf("nb: skipped = %v, want [%s]", skipped, streamerID)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts WHERE id=$1`, derived); got != 1 {
		t.Fatalf("nb: payout rows for derived id = %d, want exactly the pre-created one", got)
	}
	if got := nbMustInt64(t, db, `SELECT amount_idr FROM payouts WHERE id=$1`, derived); got != 12345 {
		t.Fatalf("nb: payout amount = %d, want 12345 (no amount drift)", got)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 0 {
		t.Fatalf("nb: batch rows = %d, want 0", got)
	}
	// The available balance was never reserved.
	if got := nbMustInt64(t, db, `SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type='STREAMER_AVAILABLE'`, streamerID); got != 25000 {
		t.Fatalf("nb: available = %d, want 25000 untouched", got)
	}
}

// Two eligible streamers sweep into ONE DRAFT batch containing both, with
// each member's amount exactly its available balance.
func TestNbRunNightlyBatchTwoEligibleStreamersOneBatch(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()
	s1, _ := nbSeedStreamer(t, db, 25000, true)
	s2, _ := nbSeedStreamer(t, db, 30000, true)

	created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil {
		t.Fatalf("nb: run err = %v", err)
	}
	if created != 2 {
		t.Fatalf("nb: created = %d, want 2", created)
	}
	if batchID == uuid.Nil {
		t.Fatal("nb: batchID is nil")
	}
	if len(skipped) != 0 {
		t.Fatalf("nb: skipped = %v, want none", skipped)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 1 {
		t.Fatalf("nb: batch rows = %d, want exactly 1 for both streamers", got)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts WHERE batch_id=$1 AND status='BATCHED'`, batchID); got != 2 {
		t.Fatalf("nb: batched members = %d, want 2 in one batch", got)
	}
	if got := nbMustInt64(t, db, `SELECT payout_count FROM payout_batches WHERE id=$1`, batchID); got != 2 {
		t.Fatalf("nb: payout_count = %d, want 2", got)
	}
	if got := nbMustInt64(t, db, `SELECT total_amount_idr FROM payout_batches WHERE id=$1`, batchID); got != 55000 {
		t.Fatalf("nb: total = %d, want 55000", got)
	}
	for _, s := range []uuid.UUID{s1, s2} {
		derived := nbDerivedPayoutID(s.String())
		if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts WHERE id=$1 AND batch_id=$2 AND status='BATCHED'`, derived, batchID); got != 1 {
			t.Fatalf("nb: member %s missing or duplicated in the batch", derived)
		}
	}
	if got := nbMustStr(t, db, `SELECT metadata->>'run_date' FROM payout_batches WHERE id=$1`, batchID); got != nbRunDate {
		t.Fatalf("nb: batch run_date = %q, want %q", got, nbRunDate)
	}
}

// The run date is the UTC date of the clock handed in: a same-instant clock
// in another zone still yields the UTC run_date in metadata and in the
// deterministic payout ids.
func TestNbRunNightlyBatchNormalizesRunDateToUTC(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()
	streamerID, _ := nbSeedStreamer(t, db, 25000, true)

	// 2026-09-30 19:00 UTC == 2026-10-01 02:00 WIB.
	now := time.Date(2026, 10, 1, 2, 0, 0, 0, time.FixedZone("WIB", 7*3600))
	created, batchID, skipped, err := RunNightlyBatch(ctx, db, now, actorAdminID)
	if err != nil {
		t.Fatalf("nb: run err = %v", err)
	}
	if created != 1 || batchID == uuid.Nil || len(skipped) != 0 {
		t.Fatalf("nb: run = (%d, %s, %v), want (1, <batch>, [])", created, batchID, skipped)
	}
	if got := nbMustStr(t, db, `SELECT metadata->>'run_date' FROM payout_batches WHERE id=$1`, batchID); got != "2026-09-30" {
		t.Fatalf("nb: run_date = %q, want 2026-09-30 (UTC)", got)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts WHERE id=$1`, keys.PayoutRequestIDKey(streamerID.String(), "nightly-sweep:2026-09-30")); got != 1 {
		t.Fatalf("nb: payout id must derive from the UTC date, got none for 2026-09-30")
	}
}

// Hard-error propagation: with the candidate's bank row locked by an outer
// transaction, CreatePayout's bank FOR UPDATE waits out lock_timeout and
// fails with a lock_timeout error — not benign — so the run aborts instead
// of skipping: err is wrapped with the candidate, created == 0, no batch, no
// payouts, and the failed serializable transaction left nothing behind.
func TestNbRunNightlyBatchHardErrorAbortsRun(t *testing.T) {
	db := nbTestDB(t)
	dsn := os.Getenv("TEST_DATABASE_URL")
	other := nbSecondConn(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	actorAdminID := uuid.New()
	streamerID, bankID := nbSeedStreamer(t, db, 25000, true)

	lockTx, err := other.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("nb: begin lock tx: %v", err)
	}
	defer lockTx.Rollback()
	if _, err := lockTx.ExecContext(ctx, `SELECT id FROM streamer_bank_accounts WHERE id=$1 FOR UPDATE`, bankID); err != nil {
		t.Fatalf("nb: lock bank row: %v", err)
	}
	// One physical connection: the GUC applies to every following statement
	// on this handle, so the blocked bank read times out instead of hanging.
	if _, err := db.ExecContext(ctx, `SET lock_timeout='400ms'`); err != nil {
		t.Fatalf("nb: set lock_timeout: %v", err)
	}

	created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err == nil {
		t.Fatal("nb: expected a hard error, got nil")
	}
	if !strings.Contains(err.Error(), "nightly-batch: create payout for "+streamerID.String()) {
		t.Fatalf("nb: err = %v, want the candidate-wrapped hard error", err)
	}
	if created != 0 {
		t.Fatalf("nb: created = %d, want 0 (abort)", created)
	}
	if batchID != uuid.Nil {
		t.Fatalf("nb: batchID = %s, want nil (abort)", batchID)
	}
	if skipped != nil {
		t.Fatalf("nb: skipped = %v, want nil (abort, not skip)", skipped)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts`); got != 0 {
		t.Fatalf("nb: payout rows after abort = %d, want 0", got)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 0 {
		t.Fatalf("nb: batch rows after abort = %d, want 0", got)
	}
}

// SKIP LOCKED semantics: with the streamer row locked by an outer
// transaction, the candidates query skips it — zero candidates, a clean
// no-op run (no error, no batch, no payout).
func TestNbRunNightlyBatchSkipLockedSkipsLockedStreamer(t *testing.T) {
	db := nbTestDB(t)
	dsn := os.Getenv("TEST_DATABASE_URL")
	other := nbSecondConn(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	actorAdminID := uuid.New()
	streamerID, _ := nbSeedStreamer(t, db, 25000, true)

	lockTx, err := other.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("nb: begin lock tx: %v", err)
	}
	defer lockTx.Rollback()
	if _, err := lockTx.ExecContext(ctx, `SELECT id FROM streamers WHERE id=$1 FOR UPDATE`, streamerID); err != nil {
		t.Fatalf("nb: lock streamer row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `SET lock_timeout='400ms'`); err != nil {
		t.Fatalf("nb: set lock_timeout: %v", err)
	}

	created, batchID, skipped, err := RunNightlyBatch(ctx, db, nbNow, actorAdminID)
	if err != nil {
		t.Fatalf("nb: run err = %v", err)
	}
	if created != 0 || batchID != uuid.Nil || len(skipped) != 0 {
		t.Fatalf("nb: run = (%d, %s, %v), want (0, nil, [])", created, batchID, skipped)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payouts`); got != 0 {
		t.Fatalf("nb: payout rows = %d, want 0", got)
	}
	if got := nbMustInt64(t, db, `SELECT COUNT(*) FROM payout_batches`); got != 0 {
		t.Fatalf("nb: batch rows = %d, want 0", got)
	}
}

// Rejecting a nil actor is validation, before any DB work.
func TestNbRunNightlyBatchNilActorRejected(t *testing.T) {
	db := nbTestDB(t)
	created, batchID, skipped, err := RunNightlyBatch(context.Background(), db, nbNow, uuid.Nil)
	if !errors.Is(err, ErrPayoutValidation) {
		t.Fatalf("nb: err = %v, want ErrPayoutValidation", err)
	}
	if created != 0 || batchID != uuid.Nil || skipped != nil {
		t.Fatalf("nb: run = (%d, %s, %v), want zero values", created, batchID, skipped)
	}
}

// The benign-vs-hard classifier behind the per-candidate skip decision.
func TestNbIsBenignNightlySkipClassification(t *testing.T) {
	benign := []error{
		ErrPayoutBelowMinimum, ErrPayoutInsufficientBalance, ErrPayoutHoldActive,
		ErrPayoutBankNotFound, ErrPayoutBankNotVerified, ErrPayoutIdempotencyConflict,
	}
	for _, be := range benign {
		if !isBenignNightlySkip(be) {
			t.Errorf("nb: %v must classify benign", be)
		}
	}
	hard := []error{
		errors.New("boom"), context.DeadlineExceeded, sql.ErrConnDone, ErrPayoutValidation,
	}
	for _, he := range hard {
		if isBenignNightlySkip(he) {
			t.Errorf("nb: %v must classify hard", he)
		}
	}
}

// --- CreateBatch metadata (C) ------------------------------------------------

// nil and empty metadata both write SQL NULL.
func TestNbCreateBatchNilAndEmptyMetadataWriteSQLNull(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()

	for _, tt := range []struct {
		name     string
		metadata map[string]string
	}{
		{"nil metadata", nil},
		{"empty metadata", map[string]string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, err := CreateBatch(ctx, db, actorAdminID, nil, tt.metadata)
			if err != nil {
				t.Fatalf("nb: create batch: %v", err)
			}
			if out.BatchID == uuid.Nil || out.PayoutCount != 0 || out.TotalAmountIDR != 0 {
				t.Fatalf("nb: out = %+v, want an empty DRAFT batch", out)
			}
			var isNull bool
			if err := db.QueryRow(`SELECT metadata IS NULL FROM payout_batches WHERE id=$1`, out.BatchID).Scan(&isNull); err != nil {
				t.Fatalf("nb: read metadata nullability: %v", err)
			}
			if !isNull {
				t.Fatalf("nb: metadata must be SQL NULL, got a value")
			}
		})
	}
}

// Non-empty metadata marshals to JSONB, and the 00017 partial unique index
// rejects a second batch carrying the same nightly run_date — while a
// different run_date (or no nightly kind) still fits the partial predicate.
func TestNbCreateBatchMetadataJsonbAndNightlyUniqueness(t *testing.T) {
	db := nbTestDB(t)
	ctx := context.Background()
	actorAdminID := uuid.New()
	nightly := map[string]string{"batch_kind": "nightly", "run_date": nbRunDate}

	out, err := CreateBatch(ctx, db, actorAdminID, nil, nightly)
	if err != nil {
		t.Fatalf("nb: create batch: %v", err)
	}
	// The JSONB payload round-trips key-by-key.
	if got := nbMustStr(t, db, `SELECT metadata->>'batch_kind' FROM payout_batches WHERE id=$1`, out.BatchID); got != "nightly" {
		t.Fatalf("nb: metadata batch_kind = %q, want nightly", got)
	}
	if got := nbMustStr(t, db, `SELECT metadata->>'run_date' FROM payout_batches WHERE id=$1`, out.BatchID); got != nbRunDate {
		t.Fatalf("nb: metadata run_date = %q, want %q", got, nbRunDate)
	}

	// Same run_date again: the partial unique index fires.
	_, err = CreateBatch(ctx, db, actorAdminID, nil, nightly)
	if err == nil {
		t.Fatal("nb: expected the duplicate same-run_date batch to violate uq_payout_batches_nightly_run")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("nb: err = %v, want a postgres unique violation (23505)", err)
	}

	// A different run_date is a different index entry: allowed.
	if _, err := CreateBatch(ctx, db, actorAdminID, nil, map[string]string{"batch_kind": "nightly", "run_date": "2026-10-02"}); err != nil {
		t.Fatalf("nb: different run_date must be allowed: %v", err)
	}
}

// CreateBatch refuses a nil creator before touching the DB.
func TestNbCreateBatchNilCreatorRejected(t *testing.T) {
	db := nbTestDB(t)
	if _, err := CreateBatch(context.Background(), db, uuid.Nil, nil, nil); !errors.Is(err, ErrPayoutValidation) {
		t.Fatalf("nb: err = %v, want ErrPayoutValidation", err)
	}
}