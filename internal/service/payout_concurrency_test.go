package service

// T26 — real-locking concurrency (06-supporter-payouts-phase-design.md §5):
// two concurrent CreatePayout calls for one streamer over one balance →
// exactly one wins the s3 reservation, the other fails INSUFFICIENT_BALANCE,
// available never goes negative, and available + paid == the seeded balance
// with the reservation posted exactly once.
//
// This is a real-Postgres test: the eligibility check and the ledger insert
// share one serializable transaction (payouts.go CreatePayout), so the
// exactly-one-winner property only exists under true SSI row locking and
// cannot be exercised through sqlmock. The test runs against an isolated
// schema (payout_cc_test) on the shared inflora_test scratch database; the
// single-connection constraint applies to schema setup only — the race itself
// runs on a second handle with default pooling so the two goroutines hold
// genuinely different connections.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-saruman/migrations"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ccSchema is the isolated schema this test lives in. Other suites share the
// public schema of inflora_test; this one must never touch it.
const ccSchema = "payout_cc_test"

// ccTestDB builds the isolated schema and returns two handles:
//   - setupDB: one pooled connection with search_path pinned, for migration,
//     seeding, and post-race verification;
//   - raceDB: default pooling with search_path carried as a pgx runtime
//     parameter, so concurrent goroutines use distinct server connections.
func ccTestDB(t *testing.T) (setupDB, raceDB *sql.DB) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()

	setupDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("cc: open setup db: %v", err)
	}
	t.Cleanup(func() { _ = setupDB.Close() })
	setupDB.SetMaxOpenConns(1)
	setupDB.SetMaxIdleConns(1)

	if _, err := setupDB.ExecContext(ctx, `DROP SCHEMA IF EXISTS `+ccSchema+` CASCADE; CREATE SCHEMA `+ccSchema); err != nil {
		t.Fatalf("cc: create schema: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, `SET search_path TO `+ccSchema); err != nil {
		t.Fatalf("cc: set search_path: %v", err)
	}
	ccApplyMigrations(t, setupDB)

	// pgx v5 passes unknown DSN query parameters through as server runtime
	// parameters, so every pooled connection of raceDB starts with the
	// isolated schema as its search_path.
	raceDSN := dsn
	if strings.Contains(raceDSN, "?") {
		raceDSN += "&search_path=" + ccSchema
	} else {
		raceDSN += "?search_path=" + ccSchema
	}
	raceDB, err = sql.Open("pgx", raceDSN)
	if err != nil {
		t.Fatalf("cc: open race db: %v", err)
	}
	t.Cleanup(func() { _ = raceDB.Close() })
	// Leave no copy behind: the isolated schema holds a full migration set and
	// would double the catalog-wide counts internal/migrate asserts on the next
	// shared-DB suite run. Registered after the Close cleanups so LIFO order
	// drops the schema while the handle is still open.
	t.Cleanup(func() {
		_, _ = setupDB.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+ccSchema+` CASCADE`)
	})
	return setupDB, raceDB
}

// ccApplyMigrations applies every non-seed up.sql from the embedded migration
// FS in version order on the setup handle, mirroring the internal/migrate
// apply loop (one transaction per migration). Any hardcoded "public."
// reference is rewritten to the isolated schema.
func ccApplyMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatalf("cc: read migration dir: %v", err)
	}
	type ccMigration struct {
		version int64
		name    string
		up      string
	}
	var ms []ccMigration
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		if len(name) < 6 || name[5] != '_' {
			t.Fatalf("cc: unexpected migration file %q", name)
		}
		version, err := strconv.ParseInt(name[:5], 10, 64)
		if err != nil {
			t.Fatalf("cc: parse migration version %q: %v", name, err)
		}
		rest := name[6 : len(name)-len(".up.sql")]
		if strings.HasPrefix(rest, "seed_") {
			continue // dev seeds are not schema and never apply here
		}
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatalf("cc: read migration %s: %v", name, err)
		}
		ms = append(ms, ccMigration{version: version, name: name, up: strings.ReplaceAll(string(body), "public.", ccSchema+".")})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	for _, m := range ms {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("cc: begin %s: %v", m.name, err)
		}
		if _, err = tx.Exec(m.up); err != nil {
			_ = tx.Rollback()
			t.Fatalf("cc: apply %s: %v", m.name, err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatalf("cc: commit %s: %v", m.name, err)
		}
	}
}

// ccSeed inserts one streamer, one verified bank account, and all seven
// ledger accounts (identical to ensurePayoutAccounts so the racers' ON
// CONFLICT DO NOTHING never contends on a missing key), with
// STREAMER_AVAILABLE funded to 100000 IDR. It also installs a
// balance-never-negative CHECK, so a transient debit past zero raises a hard
// error instead of passing silently — the T26 invariant as a DB tripwire.
func ccSeed(t *testing.T, db *sql.DB) (streamerID, bankID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	streamerID, bankID = uuid.New(), uuid.New()
	if _, err := db.ExecContext(ctx, `INSERT INTO streamers (id,email,display_name,password_hash,handle) VALUES ($1,$2,$3,$4,$5)`,
		streamerID, "cc-race-"+streamerID.String()+"@example.com", "cc-race-streamer", "x", "cc-"+streamerID.String()[:8]); err != nil {
		t.Fatalf("cc: seed streamer: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO streamer_bank_accounts (id,streamer_id,bank_code,account_number_last4,account_name,is_primary,is_verified,palantir_ref) VALUES ($1,$2,'BCA','1234','CC Race',TRUE,TRUE,'cc-palantir-ref')`,
		bankID, streamerID); err != nil {
		t.Fatalf("cc: seed bank account: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO ledger_accounts (streamer_id,account_type,currency)
		SELECT $1, account_type, 'IDR' FROM unnest(ARRAY['DONOR_CASH','STREAMER_PENDING','STREAMER_AVAILABLE','STREAMER_PAID','PLATFORM_REVENUE','MDR_REVENUE','DISBURSED']::ledger_account_type[]) account_type`, streamerID); err != nil {
		t.Fatalf("cc: seed ledger accounts: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE ledger_accounts SET balance_idr=$2 WHERE streamer_id=$1 AND account_type='STREAMER_AVAILABLE' AND currency='IDR'`, streamerID, int64(100000)); err != nil {
		t.Fatalf("cc: fund available balance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE ledger_accounts ADD CONSTRAINT cc_balance_never_negative CHECK (balance_idr >= 0)`); err != nil {
		t.Fatalf("cc: add non-negative tripwire: %v", err)
	}
	return streamerID, bankID
}

func ccMustInt64(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("cc: %s: %v", query, err)
	}
	return v
}

func ccMustStr(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var v string
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("cc: %s: %v", query, err)
	}
	return v
}

func TestCCConcurrentCreatePayoutExactlyOneWinner(t *testing.T) {
	setupDB, raceDB := ccTestDB(t)
	streamerID, bankID := ccSeed(t, setupDB)

	const each = 60000
	type ccOutcome struct {
		res CreatePayoutResult
		err error
	}
	start := make(chan struct{})
	results := make(chan ccOutcome, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("cc-race-key-%d", i)
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			<-start // release both racers at the same instant
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			res, err := CreatePayout(ctx, raceDB, CreatePayoutRequest{
				StreamerID:     streamerID,
				AmountIDR:      each,
				BankAccountID:  bankID,
				IdempotencyKey: key,
				RequestID:      uuid.New(),
			})
			results <- ccOutcome{res, err}
		}(key)
	}
	close(start)
	wg.Wait()
	close(results)

	var winners, losers []ccOutcome
	for o := range results {
		if o.err == nil {
			winners = append(winners, o)
		} else {
			losers = append(losers, o)
		}
	}
	if len(winners) != 1 || len(losers) != 1 {
		t.Fatalf("cc: expected exactly one winner, got %d winners and %d losers (winners=%+v losers=%+v)", len(winners), len(losers), winners, losers)
	}
	if !errors.Is(losers[0].err, ErrPayoutInsufficientBalance) {
		t.Fatalf("cc: loser error = %v, want ErrPayoutInsufficientBalance", losers[0].err)
	}
	if losers[0].res.PayoutID != uuid.Nil {
		t.Fatalf("cc: loser returned a payout row %s; it must have created nothing", losers[0].res.PayoutID)
	}
	winner := winners[0]
	if winner.res.PayoutID == uuid.Nil {
		t.Fatal("cc: winner returned a nil payout id")
	}
	if winner.res.Status != PayoutRequested || winner.res.AmountIDR != each || winner.res.Replay {
		t.Fatalf("cc: winner result = %+v, want status REQUESTED, amount %d, no replay", winner.res, each)
	}

	// Post-race verification runs on the single setup connection; re-pin the
	// search_path in case the pool ever recycled the session.
	ctx := context.Background()
	if _, err := setupDB.ExecContext(ctx, `SET search_path TO `+ccSchema); err != nil {
		t.Fatalf("cc: re-set search_path: %v", err)
	}

	// Exactly one payout row, REQUESTED, the winner's, at the raced amount.
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM payouts`); got != 1 {
		t.Fatalf("cc: payout rows = %d, want 1", got)
	}
	if got := ccMustStr(t, setupDB, `SELECT status::text FROM payouts`); got != PayoutRequested {
		t.Fatalf("cc: payout status = %q, want %q", got, PayoutRequested)
	}
	if got := ccMustInt64(t, setupDB, `SELECT amount_idr FROM payouts`); got != each {
		t.Fatalf("cc: payout amount = %d, want %d", got, each)
	}
	var rowID uuid.UUID
	if err := setupDB.QueryRow(`SELECT id FROM payouts`).Scan(&rowID); err != nil {
		t.Fatalf("cc: load payout id: %v", err)
	}
	if rowID != winner.res.PayoutID {
		t.Fatalf("cc: payout row id %s != winner result %s", rowID, winner.res.PayoutID)
	}

	// Ledger invariant after the race and any retries: available never
	// negative, available == 100000-60000, paid == 60000, total unchanged.
	var available, paid, total int64
	rows, err := setupDB.Query(`SELECT account_type::text, balance_idr FROM ledger_accounts WHERE streamer_id=$1`, streamerID)
	if err != nil {
		t.Fatalf("cc: read balances: %v", err)
	}
	for rows.Next() {
		var typ string
		var bal int64
		if err := rows.Scan(&typ, &bal); err != nil {
			t.Fatalf("cc: scan balance: %v", err)
		}
		if bal < 0 {
			t.Fatalf("cc: account %s went negative: %d", typ, bal)
		}
		total += bal
		switch typ {
		case "STREAMER_AVAILABLE":
			available = bal
		case "STREAMER_PAID":
			paid = bal
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("cc: iterate balances: %v", err)
	}
	if available != 100000-each {
		t.Fatalf("cc: available = %d, want %d", available, 100000-each)
	}
	if paid != each {
		t.Fatalf("cc: paid = %d, want %d", paid, each)
	}
	if total != 100000 {
		t.Fatalf("cc: total ledger balance = %d, want 100000 (reservation must net zero)", total)
	}

	// The s3 reservation posted exactly once, for the winner only: one DEBIT
	// on available (60000) and one CREDIT on paid (60000), both
	// STREAMER_PAID_DEBIT, both linked to exactly one PAYOUT_RESERVATION.
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM ledger_entries WHERE payout_id=$1`, winner.res.PayoutID); got != 2 {
		t.Fatalf("cc: winner ledger entries = %d, want 2", got)
	}
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM ledger_entries WHERE payout_id IS NOT NULL`); got != 2 {
		t.Fatalf("cc: total payout-linked entries = %d, want 2 (loser created nothing)", got)
	}
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_AVAILABLE' AND e.direction='DEBIT' AND e.amount_idr=$2 AND e.entry_type='STREAMER_PAID_DEBIT'`, winner.res.PayoutID, each); got != 1 {
		t.Fatalf("cc: available DEBIT entries = %d, want 1", got)
	}
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID' AND e.direction='CREDIT' AND e.amount_idr=$2 AND e.entry_type='STREAMER_PAID_DEBIT'`, winner.res.PayoutID, each); got != 1 {
		t.Fatalf("cc: paid CREDIT entries = %d, want 1", got)
	}
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM ledger_transactions WHERE streamer_id=$1 AND transaction_type='PAYOUT_RESERVATION'`, streamerID); got != 1 {
		t.Fatalf("cc: PAYOUT_RESERVATION transactions = %d, want exactly 1", got)
	}
	if got := ccMustInt64(t, setupDB, `SELECT COUNT(*) FROM outbox_events WHERE event_type=$1 AND streamer_id=$2`, events.PayoutRequested, streamerID.String()); got != 1 {
		t.Fatalf("cc: payout.requested outbox rows = %d, want exactly 1", got)
	}
}
