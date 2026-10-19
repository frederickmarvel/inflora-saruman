package service

// T25 — the frozen payout transition matrix (06-supporter-payouts-phase-design.md
// §"Tests"): every legal transition, every illegal one, and the service-level
// enforcement of the matrix. The pure table (payoutTransitions / CanTransition)
// is asserted exhaustively over the full 6x6 state space, then the call-site
// semantics are pinned with sqlmock:
//
//   - user/finops commands: SubmitPayout admits only REQUESTED (a PROCESSING
//     re-submit is a lease-refresh bookkeeping step, not a transition); batch
//     membership moves BATCHED<->REQUESTED and BATCHED->PROCESSING via
//     CreateBatch/AddToBatch/RemoveFromBatch/CancelBatch/SubmitBatch only;
//   - system events: withdrawal-completed/failed settle or fail only
//     PROCESSING rows, park REQUESTED/BATCHED rows in NEEDS_REVIEW (D-055 no
//     fast path; BATCHED parks detach the member per CONTRACT_DRIFT D-059),
//     and never regress SETTLED/FAILED/NEEDS_REVIEW rows;
//   - admin forced moves: RetryPayout/ForceFailPayout/ResolvePayout admit
//     only NEEDS_REVIEW (D-055/D-057 latch);
//   - sweep: REQUESTED|PROCESSING -> NEEDS_REVIEW, never auto-fail (D-057).
//
// All helpers in this file are prefixed "tr" (unique to T25); never shared
// with the sibling *_test.go files.

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

var trPayoutStatuses = []string{
	PayoutRequested, PayoutBatched, PayoutProcessing,
	PayoutSettled, PayoutFailed, PayoutNeedsReview,
}

// trLegalPairs is the frozen matrix from the design contract — an independent
// copy of payoutTransitions, so a production edit cannot loop the test to
// green (the test encodes the contract, not the implementation).
// REQUESTED -> {BATCHED, PROCESSING, NEEDS_REVIEW}; BATCHED -> {REQUESTED,
// PROCESSING, NEEDS_REVIEW (provider-event park + detach, CONTRACT_DRIFT
// D-059)}; PROCESSING -> {SETTLED, FAILED, NEEDS_REVIEW};
// NEEDS_REVIEW -> {PROCESSING, SETTLED, FAILED}; SETTLED and FAILED terminal.
var trLegalPairs = map[string]map[string]bool{
	PayoutRequested:   {PayoutBatched: true, PayoutProcessing: true, PayoutNeedsReview: true},
	PayoutBatched:     {PayoutRequested: true, PayoutProcessing: true, PayoutNeedsReview: true},
	PayoutProcessing:  {PayoutSettled: true, PayoutFailed: true, PayoutNeedsReview: true},
	PayoutNeedsReview: {PayoutProcessing: true, PayoutSettled: true, PayoutFailed: true},
	PayoutSettled:     {},
	PayoutFailed:      {},
}

// trCountLegal tallies legal pairs in the frozen matrix (must be exactly 12).
func trCountLegal(m map[string]map[string]bool) int {
	n := 0
	for _, to := range m {
		n += len(to)
	}
	return n
}

var (
	trPayoutCols = []string{
		"id", "streamer_id", "amount_idr", "status", "batch_id", "bank_account_id",
		"attempts", "requested_at", "processed_at", "settled_at", "provider_payout_id", "failure_reason",
	}
	// The real SQL shapes from payouts.go — expectations are quoted verbatim
	// so a drift in the production statement fails the test rather than
	// silently passing.
	trLockRe       = regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`)
	trLoadPayoutRe = regexp.QuoteMeta(`SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE`)
	trGatewayRe    = regexp.QuoteMeta(`SELECT EXISTS (SELECT 1 FROM gateway_withdrawals WHERE saruman_payout_id=$1)`)
	trParkRe       = regexp.QuoteMeta(`UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2 WHERE id=$1`)
	trSubmitRe     = regexp.QuoteMeta(`UPDATE payouts SET status='PROCESSING',attempts=attempts+1,processed_at=COALESCE(processed_at,NOW()),lease_expires_at=$2 WHERE id=$1 RETURNING attempts,lease_expires_at`)
	trRetryRe      = regexp.QuoteMeta(`UPDATE payouts SET status='PROCESSING',attempts=attempts+1,lease_expires_at=$2,failure_reason=NULL WHERE id=$1 RETURNING attempts,lease_expires_at`)
)

// trPayoutRow builds the payoutRow scan shape loadPayout expects. UUIDs travel
// as their string forms (the uuid Scanner accepts them), nullable columns take
// nil or a concrete value of the scanned type.
func trPayoutRow(payoutID, streamerID, status string, attempts int, batchID any, processedAt, settledAt any, providerPayoutID, failureReason any) *sqlmock.Rows {
	requestedAt := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	return sqlmock.NewRows(trPayoutCols).AddRow(
		payoutID, streamerID, int64(50000), status, batchID,
		"11111111-2222-3333-4444-555555555555", attempts, requestedAt,
		processedAt, settledAt, providerPayoutID, failureReason,
	)
}

// trExpectBeginLockLoad registers the Begin -> advisory-XACT-lock ->
// SELECT ... FOR UPDATE prologue every transition path shares.
func trExpectBeginLockLoad(mock sqlmock.Sqlmock, id string, row *sqlmock.Rows) {
	mock.ExpectBegin()
	mock.ExpectExec(trLockRe).WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(trLoadPayoutRe).WithArgs(id).WillReturnRows(row)
}

func trMustNewMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.MatchExpectationsInOrder(false)
	return db, mock
}

func trCompletedInput(payoutID, streamerID string) WithdrawalCompletedInput {
	return WithdrawalCompletedInput{
		PayoutID: uuid.MustParse(payoutID), StreamerID: uuid.MustParse(streamerID),
		ProviderPayoutID: "pp-1", BankAccountRef: "BCA-1234",
		SettledAt: time.Date(2026, 10, 18, 10, 30, 0, 0, time.UTC),
		EventID:   uuid.New(), CorrelationID: uuid.New(),
	}
}

func trFailedInput(payoutID, streamerID string) WithdrawalFailedInput {
	return WithdrawalFailedInput{
		PayoutID: uuid.MustParse(payoutID), StreamerID: uuid.MustParse(streamerID),
		FailureReason: "bank rejected", ProviderError: "ERR_REJECTED",
		FailedAt: time.Date(2026, 10, 18, 10, 40, 0, 0, time.UTC),
		EventID:  uuid.New(), CorrelationID: uuid.New(),
	}
}

// The legal half of the frozen matrix — every registered transition, with the
// driver that owns it (user/finops command, system event, admin forced move,
// sweep). CanTransition itself has no production call sites; it is the frozen
// contract surface, and the service paths must encode the same distinctions.
func TestTrCanTransitionLegalMatrix(t *testing.T) {
	tests := []struct {
		name, from, to, driver string
	}{
		{"requested to batched", PayoutRequested, PayoutBatched, "finops CreateBatch/AddToBatch claims the payout"},
		{"requested to processing", PayoutRequested, PayoutProcessing, "user command SubmitPayout (D-055 bookkeeping + lease raise)"},
		{"requested to needs_review", PayoutRequested, PayoutNeedsReview, "sweep stale lease (D-057) / provider-error park (D-055)"},
		{"batched back to requested", PayoutBatched, PayoutRequested, "finops RemoveFromBatch / CancelBatch revert"},
		{"batched to processing", PayoutBatched, PayoutProcessing, "finops SubmitBatch"},
		{"batched to needs_review", PayoutBatched, PayoutNeedsReview, "provider-event park with batch detach (CONTRACT_DRIFT D-059)"},
		{"processing to settled", PayoutProcessing, PayoutSettled, "system event withdrawal-completed, settle-time hold gate passed"},
		{"processing to failed", PayoutProcessing, PayoutFailed, "system event withdrawal-failed + s5 release"},
		{"processing to needs_review", PayoutProcessing, PayoutNeedsReview, "settle-time hold gate HOLD_CONFLICT (D-056) / sweep (D-057)"},
		{"needs_review to processing", PayoutNeedsReview, PayoutProcessing, "admin RetryPayout (fresh lease, money stays reserved)"},
		{"needs_review to settled", PayoutNeedsReview, PayoutSettled, "admin ResolvePayout settle override (D-055/D-057)"},
		{"needs_review to failed", PayoutNeedsReview, PayoutFailed, "admin ForceFailPayout + s5 release"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !CanTransition(tt.from, tt.to) {
				t.Fatalf("CanTransition(%s, %s) = false, want true (%s)", tt.from, tt.to, tt.driver)
			}
		})
	}
	if got := trCountLegal(trLegalPairs); got != 12 {
		t.Fatalf("frozen matrix has %d legal pairs, want 12", got)
	}
}

// The negative half, exhaustively: every pair not registered above must be
// refused. Named cases first for the ones the design calls out explicitly
// (terminals immutable, out-of-order jumps, double submit, batch/request
// fast paths), then a full 6x6 sweep.
func TestTrCanTransitionIllegalNamed(t *testing.T) {
	tests := []struct{ name, from, to string }{
		{"settled is terminal: re-settle", PayoutSettled, PayoutSettled},
		{"settled is terminal: fail", PayoutSettled, PayoutFailed},
		{"settled is terminal: back to processing", PayoutSettled, PayoutProcessing},
		{"settled is terminal: back to requested", PayoutSettled, PayoutRequested},
		{"settled is terminal: batch", PayoutSettled, PayoutBatched},
		{"settled is terminal: park to review", PayoutSettled, PayoutNeedsReview},
		{"failed is terminal: re-fail", PayoutFailed, PayoutFailed},
		{"failed is terminal: settle", PayoutFailed, PayoutSettled},
		{"failed is terminal: back to processing", PayoutFailed, PayoutProcessing},
		{"failed is terminal: back to requested", PayoutFailed, PayoutRequested},
		{"failed is terminal: batch", PayoutFailed, PayoutBatched},
		{"failed is terminal: park to review", PayoutFailed, PayoutNeedsReview},
		{"requested cannot jump straight to settled", PayoutRequested, PayoutSettled},
		{"requested cannot fail directly", PayoutRequested, PayoutFailed},
		{"requested self-loop unregistered", PayoutRequested, PayoutRequested},
		{"batched cannot jump straight to settled", PayoutBatched, PayoutSettled},
		{"batched cannot fail directly", PayoutBatched, PayoutFailed},
		{"batched self-loop unregistered", PayoutBatched, PayoutBatched},
		{"double submit while processing is not a registered transition", PayoutProcessing, PayoutProcessing},
		{"processing cannot revert to requested", PayoutProcessing, PayoutRequested},
		{"processing cannot be batch-claimed", PayoutProcessing, PayoutBatched},
		{"needs_review cannot revert to requested", PayoutNeedsReview, PayoutRequested},
		{"needs_review cannot be batch-claimed", PayoutNeedsReview, PayoutBatched},
		{"needs_review self-loop unregistered", PayoutNeedsReview, PayoutNeedsReview},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if CanTransition(tt.from, tt.to) {
				t.Fatalf("CanTransition(%s, %s) = true, want false", tt.from, tt.to)
			}
		})
	}
}

func TestTrCanTransitionExhaustiveMatrix(t *testing.T) {
	for _, from := range trPayoutStatuses {
		for _, to := range trPayoutStatuses {
			want := trLegalPairs[from][to]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	// The full space: 6 statuses x 6 statuses = 36 pairs, 12 legal, 24 illegal.
	if got := trCountLegal(trLegalPairs); got != 12 {
		t.Fatalf("frozen matrix has %d legal pairs, want 12", got)
	}
}

// Service-level enforcement: submitting a payout in an illegal state refuses
// with ErrPayoutIllegalTransition without touching the row. BATCHED members
// submit only via SubmitBatch and NEEDS_REVIEW rows only via RetryPayout;
// terminals never move. The only mutating statement in SubmitPayout is the
// PROCESSING UPDATE — the mock registers none, so any attempted write fails
// the expectation check.
func TestTrSubmitPayoutRejectsIllegalStates(t *testing.T) {
	for _, status := range []string{PayoutSettled, PayoutFailed, PayoutBatched, PayoutNeedsReview} {
		t.Run("from "+status, func(t *testing.T) {
			db, mock := trMustNewMock(t)
			id, streamerID := uuid.NewString(), uuid.NewString()
			trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, status, 3, nil, nil, nil, nil, nil))
			mock.ExpectRollback()

			attempts, lease, err := SubmitPayout(context.Background(), db, uuid.MustParse(id), DefaultPayoutLease)
			if !errors.Is(err, ErrPayoutIllegalTransition) {
				t.Fatalf("err = %v, want ErrPayoutIllegalTransition", err)
			}
			if attempts != 0 || !lease.IsZero() {
				t.Fatalf("illegal submit leaked partial state: attempts=%d lease=%v", attempts, lease)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The legal REQUESTED -> PROCESSING arm at service level: attempt counter,
// lease raise and gateway-row assert, status flip to PROCESSING.
func TestTrSubmitPayoutRequestedTransitionsToProcessing(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	leaseAt := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutRequested, 0, nil, nil, nil, nil, nil))
	mock.ExpectQuery(trGatewayRe).WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(trSubmitRe).WithArgs(id, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "lease_expires_at"}).AddRow(1, leaseAt))
	mock.ExpectCommit()

	attempts, lease, err := SubmitPayout(context.Background(), db, uuid.MustParse(id), DefaultPayoutLease)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if attempts != 1 || !lease.Equal(leaseAt) {
		t.Fatalf("attempts=%d lease=%v, want 1 and %v", attempts, lease, leaseAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// NOTE(deficit): "double submit" is illegal in the matrix (no registered
// PROCESSING->PROCESSING edge — asserted false above), and production agrees
// there is no transition here: the re-submission of an already-PROCESSING row
// is a deliberate lease-refresh bookkeeping step (payouts.go SubmitPayout
// switch; D-055 "SubmitPayout = bookkeeping + lease raise + gateway-row
// assert"). The status never changes — the UPDATE re-asserts status=
// 'PROCESSING' — so the matrix stays intact. This test pins the actual
// behaviour rather than weakening the illegal-pair assertion.
func TestTrSubmitPayoutProcessingResubmitRefreshesLease(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	processedAt := time.Date(2026, 10, 18, 9, 5, 0, 0, time.UTC)
	leaseAt := time.Date(2026, 10, 18, 10, 15, 0, 0, time.UTC)
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutProcessing, 2, nil, processedAt, nil, "pp-1", nil))
	mock.ExpectQuery(trGatewayRe).WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(trSubmitRe).WithArgs(id, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "lease_expires_at"}).AddRow(3, leaseAt))
	mock.ExpectCommit()

	attempts, lease, err := SubmitPayout(context.Background(), db, uuid.MustParse(id), DefaultPayoutLease)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if attempts != 3 || !lease.Equal(leaseAt) {
		t.Fatalf("attempts=%d lease=%v, want 3 and %v", attempts, lease, leaseAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Submit bookkeeping is gated on the gateway mirror row (D-055); its absence
// surfaces the frozen ErrPayoutGatewayPending before any status movement.
func TestTrSubmitPayoutRequestedGatewayRowMissing(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutRequested, 0, nil, nil, nil, nil, nil))
	mock.ExpectQuery(trGatewayRe).WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectRollback()

	_, _, err := SubmitPayout(context.Background(), db, uuid.MustParse(id), DefaultPayoutLease)
	if !errors.Is(err, ErrPayoutGatewayPending) {
		t.Fatalf("err = %v, want ErrPayoutGatewayPending", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A completion/failure event on a terminal (or already-parked) row is a
// stable idempotent no-op: never regress SETTLED, FAILED or NEEDS_REVIEW.
func TestTrWithdrawalCompletedIgnoresTerminalRows(t *testing.T) {
	for _, status := range []string{PayoutSettled, PayoutFailed, PayoutNeedsReview} {
		t.Run("from "+status, func(t *testing.T) {
			db, mock := trMustNewMock(t)
			id, streamerID := uuid.NewString(), uuid.NewString()
			trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, status, 2, nil, nil, nil, "pp-1", nil))
			mock.ExpectCommit()

			out, err := WithdrawalCompleted(context.Background(), db, trCompletedInput(id, streamerID))
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if out.Changed || out.Status != "" {
				t.Fatalf("outcome = %+v, want unchanged no-op", out)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTrWithdrawalFailedIgnoresTerminalRows(t *testing.T) {
	for _, status := range []string{PayoutSettled, PayoutFailed, PayoutNeedsReview} {
		t.Run("from "+status, func(t *testing.T) {
			db, mock := trMustNewMock(t)
			id, streamerID := uuid.NewString(), uuid.NewString()
			trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, status, 2, nil, nil, nil, "pp-1", nil))
			mock.ExpectCommit()

			out, err := WithdrawalFailed(context.Background(), db, trFailedInput(id, streamerID))
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if out.Changed || out.Status != "" {
				t.Fatalf("outcome = %+v, want unchanged no-op", out)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The event half may legally touch REQUESTED rows (REQUESTED -> NEEDS_REVIEW,
// D-055): a completed event before submit bookkeeping parks the payout for
// operator resolution with the frozen reason; reserved money stays in escrow.
func TestTrWithdrawalCompletedParksRequestedRow(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutRequested, 0, nil, nil, nil, nil, nil))
	mock.ExpectExec(trParkRe).WithArgs(id, "PROVIDER_COMPLETED_BEFORE_SUBMIT").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	out, err := WithdrawalCompleted(context.Background(), db, trCompletedInput(id, streamerID))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !out.Changed || out.Status != PayoutNeedsReview {
		t.Fatalf("outcome = %+v, want changed NEEDS_REVIEW park", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A failed provider attempt at request time parks a REQUESTED row in
// NEEDS_REVIEW with the surface-safe reason and no release — only an operator
// releases (D-055).
func TestTrWithdrawalFailedParksRequestedRow(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutRequested, 0, nil, nil, nil, nil, nil))
	mock.ExpectExec(trParkRe).WithArgs(id, "bank rejected").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	out, err := WithdrawalFailed(context.Background(), db, trFailedInput(id, streamerID))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !out.Changed || out.Status != PayoutNeedsReview {
		t.Fatalf("outcome = %+v, want changed NEEDS_REVIEW park", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// D-059: a provider event on a BATCHED member parks it AND detaches it — the
// batch counters are decremented and batch_id cleared in the same serializable
// TX, so a later DRAFT cancel never trips the payout_batches FK.
var (
	trBatchCountersRe = regexp.QuoteMeta(`UPDATE payout_batches SET total_amount_idr=total_amount_idr-$1,payout_count=payout_count-1 WHERE id=$2`)
	trDetachParkRe    = regexp.QuoteMeta(`UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2,batch_id=NULL WHERE id=$1`)
)

func TestTrWithdrawalCompletedParksBatchedRowDetaching(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID, batchID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutBatched, 0, batchID, nil, nil, nil, nil))
	mock.ExpectExec(trBatchCountersRe).WithArgs(int64(50000), batchID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(trDetachParkRe).WithArgs(id, "PROVIDER_COMPLETED_BEFORE_SUBMIT").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	out, err := WithdrawalCompleted(context.Background(), db, trCompletedInput(id, streamerID))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !out.Changed || out.Status != PayoutNeedsReview {
		t.Fatalf("outcome = %+v, want changed NEEDS_REVIEW detach park", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTrWithdrawalFailedParksBatchedRowDetaching(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID, batchID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutBatched, 0, batchID, nil, nil, nil, nil))
	mock.ExpectExec(trBatchCountersRe).WithArgs(int64(50000), batchID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(trDetachParkRe).WithArgs(id, "bank rejected").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	out, err := WithdrawalFailed(context.Background(), db, trFailedInput(id, streamerID))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !out.Changed || out.Status != PayoutNeedsReview {
		t.Fatalf("outcome = %+v, want changed NEEDS_REVIEW detach park", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Streamer ownership: an event bearing a different streamer is a not-found,
// never a transition (identity CAS before the status switch).
func TestTrWithdrawalCompletedStreamerMismatchIsNotFound(t *testing.T) {
	db, mock := trMustNewMock(t)
	id := uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, uuid.NewString(), PayoutRequested, 0, nil, nil, nil, nil, nil))
	mock.ExpectRollback()

	_, err := WithdrawalCompleted(context.Background(), db, trCompletedInput(id, uuid.NewString()))
	if !errors.Is(err, ErrPayoutNotFound) {
		t.Fatalf("err = %v, want ErrPayoutNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Admin latch (D-055/D-057): RetryPayout lifts NEEDS_REVIEW -> PROCESSING
// with a fresh lease and clears the failure reason; the money stays reserved
// throughout.
func TestTrRetryPayoutLiftsNeedsReviewToProcessing(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	leaseAt := time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC)
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutNeedsReview, 1, nil, nil, nil, nil, "HOLD_CONFLICT"))
	mock.ExpectQuery(trGatewayRe).WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(trRetryRe).WithArgs(id, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "lease_expires_at"}).AddRow(2, leaseAt))
	mock.ExpectCommit()

	attempts, lease, err := RetryPayout(context.Background(), db, uuid.MustParse(id), DefaultPayoutLease)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if attempts != 2 || !lease.Equal(leaseAt) {
		t.Fatalf("attempts=%d lease=%v, want 2 and %v", attempts, lease, leaseAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The admin paths are only legal out of NEEDS_REVIEW; from anywhere else they
// refuse without touching the row (the mock registers no UPDATE).
func TestTrRetryPayoutRejectsNonReviewStates(t *testing.T) {
	for _, status := range []string{PayoutRequested, PayoutBatched, PayoutProcessing, PayoutSettled, PayoutFailed} {
		t.Run("from "+status, func(t *testing.T) {
			db, mock := trMustNewMock(t)
			id, streamerID := uuid.NewString(), uuid.NewString()
			trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, status, 1, nil, nil, nil, nil, nil))
			mock.ExpectRollback()

			_, _, err := RetryPayout(context.Background(), db, uuid.MustParse(id), DefaultPayoutLease)
			if !errors.Is(err, ErrPayoutIllegalTransition) {
				t.Fatalf("err = %v, want ErrPayoutIllegalTransition", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTrForceFailPayoutRejectsNonReviewStates(t *testing.T) {
	for _, status := range []string{PayoutRequested, PayoutBatched, PayoutProcessing, PayoutSettled, PayoutFailed} {
		t.Run("from "+status, func(t *testing.T) {
			db, mock := trMustNewMock(t)
			id, streamerID := uuid.NewString(), uuid.NewString()
			trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, status, 1, nil, nil, nil, nil, nil))
			mock.ExpectRollback()

			_, err := ForceFailPayout(context.Background(), db, uuid.MustParse(id), "ops note")
			if !errors.Is(err, ErrPayoutIllegalTransition) {
				t.Fatalf("err = %v, want ErrPayoutIllegalTransition", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTrResolvePayoutRejectsNonReviewState(t *testing.T) {
	db, mock := trMustNewMock(t)
	id, streamerID := uuid.NewString(), uuid.NewString()
	trExpectBeginLockLoad(mock, id, trPayoutRow(id, streamerID, PayoutRequested, 0, nil, nil, nil, nil, nil))
	mock.ExpectRollback()

	_, err := ResolvePayout(context.Background(), db, uuid.MustParse(id), PayoutSettled)
	if !errors.Is(err, ErrPayoutIllegalTransition) {
		t.Fatalf("err = %v, want ErrPayoutIllegalTransition", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ResolvePayout validates the target literal before any DB work: the frozen
// admin override accepts exactly SETTLED or FAILED.
func TestTrResolvePayoutRejectsUnknownTarget(t *testing.T) {
	for _, target := range []string{"", "maybe", "processing"} {
		t.Run("target "+target, func(t *testing.T) {
			// nil db: the validation must refuse before the handle is used.
			_, err := ResolvePayout(context.Background(), nil, uuid.New(), target)
			if !errors.Is(err, ErrPayoutValidation) {
				t.Fatalf("err = %v, want ErrPayoutValidation", err)
			}
		})
	}
}

// surfaceSafeReason folds provider input into the frozen VARCHAR(80)
// failure_reason without leaking provider internals.
func TestTrSurfaceSafeReason(t *testing.T) {
	t.Run("empty reason defaults to provider failure", func(t *testing.T) {
		if got := surfaceSafeReason("", "internal stack trace"); got != "PROVIDER_FAILURE" {
			t.Fatalf("got %q, want PROVIDER_FAILURE", got)
		}
	})
	t.Run("explicit reason is kept verbatim", func(t *testing.T) {
		if got := surfaceSafeReason("bank rejected", "ERR_X"); got != "bank rejected" {
			t.Fatalf("got %q, want bank rejected", got)
		}
	})
	t.Run("truncates at eighty bytes of the reason", func(t *testing.T) {
		long := strings.Repeat("x", 100)
		got := surfaceSafeReason(long, "")
		if len(got) != 80 || !strings.HasPrefix(got, strings.Repeat("x", 80)) {
			t.Fatalf("got %d chars, want exactly 80", len(got))
		}
	})
}
