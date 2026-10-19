package service

// T29 — sweep + provider-state tests (06-supporter-payouts-phase-design.md
// §"Tests", line 147), the fixed semantics from D-055/D-057:
//
//   - the lease sweep flags REQUESTED and PROCESSING rows past the 2x-lease
//     threshold regardless of the attempts column, parks them in
//     NEEDS_REVIEW, folds the gateway mirror's last known provider status
//     into the failure reason class, and never auto-fails / never releases
//     money / never auto-resolves (sweep.go);
//   - provider-error data folds into NEEDS_REVIEW or FAILED through the
//     frozen PROCESSING->{SETTLED,FAILED} event semantics, with the
//     surfaceSafeReason seam guaranteeing the reason text is a class, never
//     raw provider text;
//   - admin ForceFailPayout (FAILED + s5 release, failure_reason recorded)
//     and ResolvePayout (s4 completion + SETTLED);
//   - RefreshProviderState applies the mirror truth: COMPLETED settles
//     (PROCESSING->SETTLED), FAILED fails/park per the row state, anything
//     else is a no-op refresh.
//
// All helpers in this file are prefixed "sw" (unique to T29); never shared
// with the sibling *_test.go files.
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/frederickmarvel/inflora-saruman/internal/ledger"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
)

// --- sw helpers -------------------------------------------------------------

// swArgFunc adapts a plain predicate to the sqlmock.Argument interface. The
// predicate receives the argument as the database/sql default converter left
// it (uuid.UUID -> its Valuer string, []byte stays []byte, time.Time stays
// time.Time, nil stays nil).
type swArgFunc func(driver.Value) bool

func (f swArgFunc) Match(v driver.Value) bool { return f(v) }

// swCapture records the argument's converted driver value (without ever
// rejecting it); swEqCaptured later pins a different argument to the same
// value, linking the ledger transaction row id across the s4/s5 INSERTs.
func swCapture(out *any) sqlmock.Argument {
	return swArgFunc(func(v driver.Value) bool { *out = v; return true })
}

func swEqCaptured(p *any) sqlmock.Argument {
	return swArgFunc(func(v driver.Value) bool { return reflect.DeepEqual(*p, v) })
}

// swNilOrZeroUUID matches either SQL NULL or the converted form of uuid.Nil
// (its Valuer serialises to the all-zero hex text). The frozen paths that
// lack a provider event id pass uuid.Nil and the driver ships the zero text.
func swNilOrZeroUUID() sqlmock.Argument {
	return swArgFunc(func(v driver.Value) bool {
		if v == nil {
			return true
		}
		s, ok := v.(string)
		return ok && s == uuid.Nil.String()
	})
}

// swTimeWindow matches a time.Time argument that lands within tol of
// now.Add(-2*lease) — the sweep cutoff computed from the caller's lease.
func swTimeWindow(t *testing.T, lease time.Duration) sqlmock.Argument {
	t.Helper()
	return swArgFunc(func(v driver.Value) bool {
		got, ok := v.(time.Time)
		if !ok {
			return false
		}
		want := time.Now().UTC().Add(-2 * lease)
		d := got.Sub(want)
		if d < 0 {
			d = -d
		}
		return d <= 30*time.Second
	})
}

// swQuoted returns the regexp.QuoteMeta'd distinctive SQL prefix, matching
// the established sqlmock idiom in this package.
func swQuoted(sqlPrefix string) string { return regexp.QuoteMeta(sqlPrefix) }

// swMeta marshals a ledger_transactions metadata map the way the posting
// writers do (Go's json.Marshal sorts map keys, so bytes are exact).
func swMeta(kv map[string]string) []byte {
	b, err := json.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return b
}

// swNewMock builds a mock with strict ordering — every production statement
// must be pre-registered, so an un-registered write (e.g. a stray ledger
// posting the frozen semantics forbid) fails the test at call time.
func swNewMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.MatchExpectationsInOrder(true)
	return db, mock
}

// swNewSweepMock builds a mock whose matcher additionally enforces the frozen
// D-057 sweep constraints on every statement that crosses it:
//
//   - no reference to the attempts column (flagging is attempt-agnostic);
//   - no 'FAILED' literal (the sweep never auto-fails);
//   - no ledger_* table (the sweep never releases money);
//   - no outbox_events (the shipped flagging mechanism is the bare
//     status+failure_reason UPDATE — sweep.go emits no event; the design
//     mandates the status change only).
func swNewSweepMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	guard := sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
		if strings.Contains(actualSQL, "p.status IN") && strings.Contains(actualSQL, "attempts") {
			return fmt.Errorf("D-057: sweep SELECT must not filter on attempts: %s", actualSQL)
		}
		if strings.Contains(actualSQL, "'FAILED'") {
			return fmt.Errorf("D-057: sweep must never write FAILED (never auto-fail): %s", actualSQL)
		}
		if strings.Contains(actualSQL, "ledger_") {
			return fmt.Errorf("D-057: sweep must never touch the ledger (money stays reserved): %s", actualSQL)
		}
		if strings.Contains(actualSQL, "outbox_events") {
			return fmt.Errorf("sweep emits no outbox event (shipped mechanism is status+failure_reason only): %s", actualSQL)
		}
		return sqlmock.QueryMatcherRegexp.Match(expectedSQL, actualSQL)
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(guard))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.MatchExpectationsInOrder(true)
	return db, mock
}

// swPayoutCols mirrors loadPayout's SELECT column order exactly.
var swPayoutCols = []string{"id", "streamer_id", "amount_idr", "status", "batch_id", "bank_account_id", "attempts", "requested_at", "processed_at", "settled_at", "provider_payout_id", "failure_reason"}

// swPayoutRow builds the FOR UPDATE row loadPayout scans. batchID, provider
// payout id and failure reason take nil for NULL.
func swPayoutRow(id, streamerID uuid.UUID, amount int64, status string, batchID, providerPayoutID, failureReason any) *sqlmock.Rows {
	requestedAt := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	processedAt := time.Date(2026, 10, 18, 9, 5, 0, 0, time.UTC)
	return sqlmock.NewRows(swPayoutCols).AddRow(
		id.String(), streamerID.String(), amount, status, batchID,
		"11111111-2222-3333-4444-555555555555", 1, requestedAt,
		processedAt, nil, providerPayoutID, failureReason,
	)
}

// The real SQL shapes from sweep.go / payouts.go / payout_postings.go.
var (
	swLockRe         = swQuoted(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`)
	swLoadRe         = swQuoted(`SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE`)
	swHoldGateRe     = swQuoted(`SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))`)
	swAccountsRe     = swQuoted(`INSERT INTO ledger_accounts (streamer_id,account_type,currency)`)
	swAcctLoadRe     = swQuoted(`SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE`)
	swBalLoadRe      = swQuoted(`SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE`)
	swEntryRe        = swQuoted(`INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES`)
	swBalUpdateRe    = swQuoted(`UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2`)
	swEscrowRe       = swQuoted(`SELECT COALESCE(SUM(CASE WHEN e.direction='DEBIT' THEN -e.amount_idr ELSE e.amount_idr END),0) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID'`)
	swReserveTxRe    = swQuoted(`SELECT id FROM ledger_transactions WHERE operation_key=$1`)
	swReserveEntryRe = swQuoted(`SELECT e.id FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID' AND e.entry_type='STREAMER_PAID_DEBIT' ORDER BY e.created_at LIMIT 1`)
	swReleaseTxRe    = swQuoted(`INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,reversal_of,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_FAILURE_RELEASE'`)
	swCompleteTxRe   = swQuoted(`INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION'`)
	swOutboxRe       = swQuoted(`INSERT INTO outbox_events (`)
	swParkRe         = swQuoted(`UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2 WHERE id=$1`)
	swSweepUpdateRe  = swQuoted(`UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2 WHERE id=$1 AND status IN ('REQUESTED','PROCESSING')`)
	swSweepSelectRe  = swQuoted(`SELECT p.id, COALESCE(gw.last_known_provider_status,'')`)
	swMirrorRe       = swQuoted(`SELECT COALESCE(gw.last_known_provider_status,''),COALESCE(gw.provider_payout_id,''),COALESCE(gw.bank_account_ref,''),COALESCE(p.status::text,'') FROM gateway_withdrawals gw JOIN payouts p ON p.id=gw.saruman_payout_id WHERE gw.saruman_payout_id=$1 ORDER BY gw.created_at DESC LIMIT 1`)
	swStreamerRe     = swQuoted(`SELECT streamer_id FROM payouts WHERE id=$1`)
)

// swExpectBeginLockLoad registers the Begin -> advisory-XACT-lock ->
// SELECT ... FOR UPDATE prologue the transition paths share.
func swExpectBeginLockLoad(t *testing.T, mock sqlmock.Sqlmock, payoutID uuid.UUID, row *sqlmock.Rows) {
	t.Helper()
	mock.ExpectBegin()
	mock.ExpectExec(swLockRe).WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(swLoadRe).WithArgs(payoutID.String()).WillReturnRows(row)
}

// swExpectRelease registers the full frozen §5 failure release for amount:
// reservation-transaction lookup (op key create_payout:<id>), s3 paid-credit
// entry lookup, the PAYOUT_FAILURE_RELEASE transaction whose reversal_of
// links the reservation transaction (level 1), DEBIT STREAMER_PAID whose
// entry reversal_of links the s3 paid-credit entry (level 2, both entries
// sharing the new transaction id), CREDIT STREAMER_AVAILABLE restoring the
// exact amount, and the escrow-net-zero invariant check. It returns the
// captured release transaction id.
func swExpectRelease(t *testing.T, mock sqlmock.Sqlmock, payoutID, streamerID uuid.UUID, amount int64, reserveTxID, reservePaidEntryID uuid.UUID, releaseMeta []byte) *any {
	t.Helper()
	var relTxID any
	mock.ExpectQuery(swReserveTxRe).
		WithArgs(keys.CreatePayoutKey(payoutID.String())).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(reserveTxID.String()))
	mock.ExpectQuery(swReserveEntryRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(reservePaidEntryID.String()))
	mock.ExpectExec(swReleaseTxRe).
		WithArgs(swCapture(&relTxID), keys.FailPayoutKey(payoutID.String()), nil,
			streamerID.String(), reserveTxID.String(), releaseMeta, ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))
	paidAcctID, availAcctID := uuid.NewString(), uuid.NewString()
	mock.ExpectQuery(swAcctLoadRe).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(swEntryRe).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), reservePaidEntryID.String(), nil, swEqCaptured(&relTxID), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(swBalUpdateRe).
		WithArgs(-amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(swAcctLoadRe).
		WithArgs(streamerID.String(), "STREAMER_AVAILABLE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(availAcctID))
	mock.ExpectExec(swEntryRe).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), availAcctID, "CREDIT", amount, "STREAMER_AVAILABLE_CREDIT",
			payoutID.String(), payoutID.String(), nil, nil, swEqCaptured(&relTxID), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(swBalUpdateRe).
		WithArgs(amount, availAcctID).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(swEscrowRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"net"}).AddRow(int64(0)))
	return &relTxID
}

// swExpectCompletion registers the frozen §4 completion tail for amount:
// the PAYOUT_COMPLETION transaction under op key complete_payout:<id>, the
// paid-escrow sufficiency guard, terminal DEBIT STREAMER_PAID
// (STREAMER_PAID_DEBIT) and CREDIT DISBURSED (STREAMER_DISBURSED_CREDIT)
// sharing the captured transaction id — available is never touched — and the
// escrow-net-zero invariant. completedMeta must carry the exact
// {"provider_payout_id":..., "source":...} bytes the writer marshals.
func swExpectCompletion(t *testing.T, mock sqlmock.Sqlmock, payoutID, streamerID uuid.UUID, amount int64, completedMeta []byte) *any {
	t.Helper()
	var compTxID any
	mock.ExpectExec(swCompleteTxRe).
		WithArgs(swCapture(&compTxID), keys.CompletePayoutKey(payoutID.String()), nil,
			streamerID.String(), completedMeta, ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(swBalLoadRe).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(999999)))
	paidAcctID, disbAcctID := uuid.NewString(), uuid.NewString()
	mock.ExpectQuery(swAcctLoadRe).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(swEntryRe).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, nil, swEqCaptured(&compTxID), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(swBalUpdateRe).
		WithArgs(-amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(swAcctLoadRe).
		WithArgs(streamerID.String(), "DISBURSED").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(disbAcctID))
	mock.ExpectExec(swEntryRe).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), disbAcctID, "CREDIT", amount, "STREAMER_DISBURSED_CREDIT",
			payoutID.String(), payoutID.String(), nil, nil, swEqCaptured(&compTxID), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(swBalUpdateRe).
		WithArgs(amount, disbAcctID).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(swEscrowRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"net"}).AddRow(int64(0)))
	return &compTxID
}

// swExpectOutbox registers the payout-family outbox INSERT with the frozen
// column values pinning event type and streamer-scoped subject.
func swExpectOutbox(t *testing.T, mock sqlmock.Sqlmock, eventType string, payoutID, streamerID uuid.UUID) {
	t.Helper()
	mock.ExpectExec(swOutboxRe).
		WithArgs(sqlmock.AnyArg(), "saruman", eventType, "1", "payout",
			payoutID.String(), streamerID.String(), eventType+"."+streamerID.String(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
}

// --- 1. sweep flagging (D-057) ----------------------------------------------

// TestSwSweepFlagsStaleRequestedAndProcessingRegardlessOfAttempts pins the
// frozen sweep: REQUESTED and PROCESSING rows past the 2x-lease threshold are
// parked in NEEDS_REVIEW with reason STALE_PROVIDER (provider mirror absent
// or still in flight), the update is CAS-guarded on the non-terminal status,
// and the count of flagged rows is reported. The selection criterion never
// references the attempts column — the attempts-agnostic flagging is pinned
// by the verbatim SELECT expectation plus the guard matcher — so attempts=0
// and attempts>0 rows are swept identically. Passing lease<=0 falls back to
// DefaultPayoutLease (asserted via the 2x threshold on the cutoff argument).
func TestSwSweepFlagsStaleRequestedAndProcessingRegardlessOfAttempts(t *testing.T) {
	tests := []struct {
		name  string
		lease time.Duration
	}{
		{"default lease window", 0},
		{"explicit lease window", 7 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantLease := DefaultPayoutLease
			if tt.lease > 0 {
				wantLease = tt.lease
			}
			db, mock := swNewSweepMock(t)

			reqA, reqB, procC := uuid.New(), uuid.New(), uuid.New()
			mock.ExpectBegin()
			mock.ExpectQuery(swSweepSelectRe).
				WithArgs(swTimeWindow(t, wantLease)).
				WillReturnRows(sqlmock.NewRows([]string{"id", "last_known_provider_status"}).
					AddRow(reqA.String(), "").
					AddRow(reqB.String(), "PROCESSING").
					AddRow(procC.String(), "FAILED"))
			mock.ExpectExec(swSweepUpdateRe).
				WithArgs(reqA.String(), "STALE_PROVIDER").
				WillReturnResult(driver.RowsAffected(1))
			mock.ExpectExec(swSweepUpdateRe).
				WithArgs(reqB.String(), "STALE_PROVIDER").
				WillReturnResult(driver.RowsAffected(1))
			mock.ExpectExec(swSweepUpdateRe).
				WithArgs(procC.String(), "PROVIDER_STATUS: FAILED").
				WillReturnResult(driver.RowsAffected(1))
			mock.ExpectCommit()

			flagged, err := SweepPayouts(context.Background(), db, tt.lease)
			if err != nil {
				t.Fatal(err)
			}
			if flagged != 3 {
				t.Fatalf("flagged = %d, want 3 (REQUESTED x2, PROCESSING x1)", flagged)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestSwSweepFoldsProviderStatusIntoReasonClass pins the provider-error
// folding (D-057): the mirror's last known provider status lands behind the
// PROVIDER_STATUS: class prefix, never bare provider text, capped at the
// frozen 80-char failure_reason width.
func TestSwSweepFoldsProviderStatusIntoReasonClass(t *testing.T) {
	db, mock := swNewSweepMock(t)

	reqA, procB := uuid.New(), uuid.New()
	longStatus := strings.Repeat("X", 100)
	wantB := "PROVIDER_STATUS: " + longStatus
	wantB = wantB[:80] // frozen VARCHAR(200)... failure_reason width of 80 in sweep.go

	mock.ExpectBegin()
	mock.ExpectQuery(swSweepSelectRe).
		WithArgs(swTimeWindow(t, DefaultPayoutLease)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "last_known_provider_status"}).
			AddRow(reqA.String(), "NOT_IMPLEMENTED").
			AddRow(procB.String(), longStatus))
	mock.ExpectExec(swSweepUpdateRe).
		WithArgs(reqA.String(), "PROVIDER_STATUS: NOT_IMPLEMENTED").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(swSweepUpdateRe).
		WithArgs(procB.String(), wantB).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	flagged, err := SweepPayouts(context.Background(), db, DefaultPayoutLease)
	if err != nil {
		t.Fatal(err)
	}
	if flagged != 2 {
		t.Fatalf("flagged = %d, want 2", flagged)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestSwSweepNoStaleRowsIsNoop: an empty stale set writes nothing — no
// UPDATE fires (an unexpected one would fail the strict mock) and the sweep
// reports zero.
func TestSwSweepNoStaleRowsIsNoop(t *testing.T) {
	db, mock := swNewSweepMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(swSweepSelectRe).
		WithArgs(swTimeWindow(t, DefaultPayoutLease)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "last_known_provider_status"}))
	mock.ExpectCommit()

	flagged, err := SweepPayouts(context.Background(), db, DefaultPayoutLease)
	if err != nil {
		t.Fatal(err)
	}
	if flagged != 0 {
		t.Fatalf("flagged = %d, want 0", flagged)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestSwSweeperContract pins the worker identity.
func TestSwSweeperContract(t *testing.T) {
	if got := (&Sweeper{}).Name(); got != "payout-sweeper" {
		t.Fatalf("Name() = %q, want payout-sweeper", got)
	}
}

// --- 2. admin force-fail: FAILED + s5 release --------------------------------

// TestSwForceFailPostsReleaseAndRecordsClassedReason pins the admin
// ForceFailPayout: a NEEDS_REVIEW payout goes FAILED with the operator note
// folded into the classed reason, the s5 release posts the full reversal
// linkage (transaction reversal_of -> the s3 reservation transaction; entry
// reversal_of -> the s3 paid-credit entry), the exact amount returns to
// STREAMER_AVAILABLE, the paid escrow nets to zero, and payout.failed.v1 is
// enqueued.
func TestSwForceFailPostsReleaseAndRecordsClassedReason(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()
	const amount = int64(50000)
	reserveTxID, reservePaidEntryID := uuid.New(), uuid.New()

	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, amount, PayoutNeedsReview, nil, "pp-9", "HOLD_CONFLICT"))
	swExpectRelease(t, mock, payoutID, streamerID, amount, reserveTxID, reservePaidEntryID,
		swMeta(map[string]string{"source": events.PgGatewayWithdrawalFailed}))
	mock.ExpectExec(swQuoted(`UPDATE payouts SET status='FAILED',failure_reason=$2 WHERE id=$1`)).
		WithArgs(payoutID.String(), "FORCE_FAILED: streamer closed account").
		WillReturnResult(driver.RowsAffected(1))
	swExpectOutbox(t, mock, events.PayoutFailed, payoutID, streamerID)
	mock.ExpectCommit()

	out, err := ForceFailPayout(context.Background(), db, payoutID, "streamer closed account")
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutFailed || !out.Changed || out.PayoutID != payoutID {
		t.Fatalf("outcome = %+v, want FAILED changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestSwResolveFailDelegatesToForceFail: resolving a NEEDS_REVIEW payout to
// FAILED takes the ForceFailPayout path with the bare FORCE_FAILED reason
// (no note).
func TestSwResolveFailDelegatesToForceFail(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()
	const amount = int64(50000)
	reserveTxID, reservePaidEntryID := uuid.New(), uuid.New()

	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, amount, PayoutNeedsReview, nil, nil, "PROVIDER_STATUS: FAILED"))
	swExpectRelease(t, mock, payoutID, streamerID, amount, reserveTxID, reservePaidEntryID,
		swMeta(map[string]string{"source": events.PgGatewayWithdrawalFailed}))
	mock.ExpectExec(swQuoted(`UPDATE payouts SET status='FAILED',failure_reason=$2 WHERE id=$1`)).
		WithArgs(payoutID.String(), "FORCE_FAILED").
		WillReturnResult(driver.RowsAffected(1))
	swExpectOutbox(t, mock, events.PayoutFailed, payoutID, streamerID)
	mock.ExpectCommit()

	out, err := ResolvePayout(context.Background(), db, payoutID, PayoutFailed)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutFailed || !out.Changed {
		t.Fatalf("outcome = %+v, want FAILED changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- 3. admin resolve: s4 completion + SETTLED -------------------------------

// TestSwResolveSettlePostsCompletionAndSettles pins the operator settle
// override: the NEEDS_REVIEW payout posts the s4 completion (paid-escrow
// DEBIT + DISBURSED CREDIT, STREAMER_AVAILABLE untouched), carries the stored
// provider payout id onto the row, and lands SETTLED with payout.settled.v1.
func TestSwResolveSettlePostsCompletionAndSettles(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()
	const amount = int64(50000)

	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, amount, PayoutNeedsReview, nil, "pp-123", "HOLD_CONFLICT"))
	mock.ExpectExec(swAccountsRe).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	swExpectCompletion(t, mock, payoutID, streamerID, amount,
		swMeta(map[string]string{"provider_payout_id": "pp-123", "source": events.PgGatewayWithdrawalCompleted}))
	mock.ExpectExec(swQuoted(`UPDATE payouts SET status='SETTLED',settled_at=$2,provider_payout_id=$3 WHERE id=$1`)).
		WithArgs(payoutID.String(), sqlmock.AnyArg(), "pp-123").
		WillReturnResult(driver.RowsAffected(1))
	swExpectOutbox(t, mock, events.PayoutSettled, payoutID, streamerID)
	mock.ExpectCommit()

	out, err := ResolvePayout(context.Background(), db, payoutID, PayoutSettled)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutSettled || !out.Changed || out.PayoutID != payoutID || out.SettledAt.IsZero() {
		t.Fatalf("outcome = %+v, want SETTLED changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- 4. RefreshProviderState ------------------------------------------------

// TestSwRefreshCompletedGatewaySettlesProcessing pins the frozen
// PROCESSING->SETTLED event path through the admin refresh: a COMPLETED
// mirror row drives WithdrawalCompleted (hold gate re-check, s4 completion,
// status guard). The refresh passes no provider event id, so last_event_id
// travels as the zero/uuid text — pinned by swNilOrZeroUUID.
func TestSwRefreshCompletedGatewaySettlesProcessing(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()
	const amount = int64(50000)

	// Refresh reads the mirror outside any transaction.
	mock.ExpectQuery(swMirrorRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"last_known_provider_status", "provider_payout_id", "bank_account_ref", "status"}).
			AddRow("COMPLETED", "gw-77", "BCA-9999", "PROCESSING"))
	mock.ExpectQuery(swStreamerRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"streamer_id"}).AddRow(streamerID.String()))

	// WithdrawalCompleted: PROCESSING -> SETTLED with the settle-time hold
	// gate and the full s4 completion.
	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, amount, PayoutProcessing, nil, nil, nil))
	mock.ExpectQuery(swHoldGateRe).
		WithArgs(streamerID.String(), payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(swAccountsRe).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	swExpectCompletion(t, mock, payoutID, streamerID, amount,
		swMeta(map[string]string{"provider_payout_id": "gw-77", "source": events.PgGatewayWithdrawalCompleted}))
	mock.ExpectExec(swQuoted(`UPDATE payouts SET status='SETTLED',settled_at=$2,provider_payout_id=$3,last_event_id=$4 WHERE id=$1 AND status='PROCESSING'`)).
		WithArgs(payoutID.String(), sqlmock.AnyArg(), "gw-77", swNilOrZeroUUID()).
		WillReturnResult(driver.RowsAffected(1))
	swExpectOutbox(t, mock, events.PayoutSettled, payoutID, streamerID)
	mock.ExpectCommit()

	out, err := RefreshProviderState(context.Background(), db, payoutID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutSettled || !out.Changed || out.SettledAt.IsZero() {
		t.Fatalf("outcome = %+v, want SETTLED changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestSwRefreshProviderFailureParksRequestedWithoutRelease pins the frozen
// D-055 park: provider truth FAILED on a REQUESTED row parks it in
// NEEDS_REVIEW with the classed reason — the safety net, not the terminal —
// and posts NOTHING (no release: the money stays reserved; strict mock
// expectations make any ledger write fail).
func TestSwRefreshProviderFailureParksRequestedWithoutRelease(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()

	// The mirror row carries the payout's own status in the fourth column;
	// surfaceSafeReason drops it — the failure reason stays the frozen class.
	mock.ExpectQuery(swMirrorRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"last_known_provider_status", "provider_payout_id", "bank_account_ref", "status"}).
			AddRow("FAILED", "", "", "REQUESTED"))
	mock.ExpectQuery(swStreamerRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"streamer_id"}).AddRow(streamerID.String()))

	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, int64(50000), PayoutRequested, nil, nil, nil))
	mock.ExpectExec(swParkRe).
		WithArgs(payoutID.String(), "PROVIDER_STATUS_FAILED").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	out, err := RefreshProviderState(context.Background(), db, payoutID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutNeedsReview || !out.Changed {
		t.Fatalf("outcome = %+v, want NEEDS_REVIEW changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestSwRefreshProviderFailureFailsProcessingWithRelease pins the FAILED
// provider truth on a PROCESSING row: the frozen PROCESSING->FAILED path with
// the s5 release; failure_reason is the class PROVIDER_STATUS_FAILED — the
// mirrored payout status (which the refresh passes as the provider error)
// travels only into the release metadata, never into the surfaced reason.
func TestSwRefreshProviderFailureFailsProcessingWithRelease(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()
	const amount = int64(50000)
	reserveTxID, reservePaidEntryID := uuid.New(), uuid.New()

	mock.ExpectQuery(swMirrorRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"last_known_provider_status", "provider_payout_id", "bank_account_ref", "status"}).
			AddRow("FAILED", "gw-88", "BCA-8888", "PROCESSING"))
	mock.ExpectQuery(swStreamerRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"streamer_id"}).AddRow(streamerID.String()))

	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, amount, PayoutProcessing, nil, nil, nil))
	swExpectRelease(t, mock, payoutID, streamerID, amount, reserveTxID, reservePaidEntryID,
		swMeta(map[string]string{"provider_error": "PROCESSING", "source": events.PgGatewayWithdrawalFailed}))
	mock.ExpectExec(swQuoted(`UPDATE payouts SET status='FAILED',failure_reason=$2,last_event_id=$3 WHERE id=$1 AND status='PROCESSING'`)).
		WithArgs(payoutID.String(), "PROVIDER_STATUS_FAILED", swNilOrZeroUUID()).
		WillReturnResult(driver.RowsAffected(1))
	swExpectOutbox(t, mock, events.PayoutFailed, payoutID, streamerID)
	mock.ExpectCommit()

	out, err := RefreshProviderState(context.Background(), db, payoutID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutFailed || !out.Changed {
		t.Fatalf("outcome = %+v, want FAILED changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestSwRefreshUnknownStatusIsNoop: a PROCESSING/unknown mirror status
// applies nothing — the refresh returns an unchanged outcome and issues no
// further statements (the strict ordered mock fails on any extra call).
func TestSwRefreshUnknownStatusIsNoop(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID := uuid.New()

	mock.ExpectQuery(swMirrorRe).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"last_known_provider_status", "provider_payout_id", "bank_account_ref", "status"}).
			AddRow("PROCESSING", "gw-55", "BCA-5555", "PROCESSING"))

	out, err := RefreshProviderState(context.Background(), db, payoutID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Changed || out.Status != "" || out.PayoutID != payoutID {
		t.Fatalf("outcome = %+v, want untouched no-op", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- 5. surfaceSafeReason seam (frozen rule) --------------------------------

// TestSwSurfaceSafeReasonClassOnly pins the frozen reason classes: an empty
// reason defaults to PROVIDER_FAILURE, the provider error text NEVER enters
// the failure reason, and the frozen 80-char cap applies.
func TestSwSurfaceSafeReasonClassOnly(t *testing.T) {
	if got := surfaceSafeReason("", "midtrans iris: operation not implemented (internal trace)"); got != "PROVIDER_FAILURE" {
		t.Fatalf("surfaceSafeReason default = %q, want PROVIDER_FAILURE", got)
	}
	if got := surfaceSafeReason("BANK_REJECTED", "midtrans iris: operation not implemented (internal trace)"); got != "BANK_REJECTED" {
		t.Fatalf("surfaceSafeReason kept class = %q, want BANK_REJECTED", got)
	}
	long := strings.Repeat("R", 100)
	got := surfaceSafeReason(long, "raw provider internals")
	if len(got) != 80 || !strings.HasPrefix(got, "RRRR") || strings.Contains(got, "provider internals") {
		t.Fatalf("surfaceSafeReason truncation = %q (len %d), want 80-char prefix only", got, len(got))
	}
}

// TestSwWithdrawalFailedParksRequestedWithGenericClass pins the D-055
// ErrNotImplemented fold through the withdrawal-failed event: a REQUESTED row
// parks in NEEDS_REVIEW with the generic PROVIDER_FAILURE class even when the
// provider error carries raw internals — the surfaced reason is the class,
// never provider text, and no release is posted.
func TestSwWithdrawalFailedParksRequestedWithGenericClass(t *testing.T) {
	db, mock := swNewMock(t)
	payoutID, streamerID := uuid.New(), uuid.New()

	swExpectBeginLockLoad(t, mock, payoutID,
		swPayoutRow(payoutID, streamerID, int64(50000), PayoutRequested, nil, nil, nil))
	mock.ExpectExec(swParkRe).
		WithArgs(payoutID.String(), "PROVIDER_FAILURE").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	out, err := WithdrawalFailed(context.Background(), db, WithdrawalFailedInput{
		PayoutID:      payoutID,
		StreamerID:    streamerID,
		FailureReason: "",
		ProviderError: "ErrNotImplemented: midtrans iris withdrawal unsupported (line 42)",
		FailedAt:      time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutNeedsReview || !out.Changed {
		t.Fatalf("outcome = %+v, want NEEDS_REVIEW changed", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
