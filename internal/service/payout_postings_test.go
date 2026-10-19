// Payout ledger-posting posture tests (T24, 06-supporter-payouts-phase-design.md §5).
// These pin the frozen posting templates end-to-end through CreatePayout /
// WithdrawalCompleted / WithdrawalFailed:
//
//	s3 reserve:   PAYOUT_RESERVATION, DEBIT available + CREDIT paid (STREAMER_PAID_DEBIT)
//	s4 complete:  PAYOUT_COMPLETION, DEBIT paid (STREAMER_PAID_DEBIT) + CREDIT DISBURSED
//	              (STREAMER_DISBURSED_CREDIT, D-048) — available untouched
//	s5 release:   PAYOUT_FAILURE_RELEASE, reversal linkage at both levels, net zero
//
// plus op-key idempotency, account-hierarchy rejection, settle-time hold gate
// (D-056) and the serializable hold TOCTOU retry. Helper prefix: ph.
package service

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/frederickmarvel/inflora-saruman/internal/ledger"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- ph helpers ------------------------------------------------------------

// phpayoutCols mirrors loadPayout's SELECT column order exactly.
var phpayoutCols = []string{"id", "streamer_id", "amount_idr", "status", "batch_id", "bank_account_id", "attempts", "requested_at", "processed_at", "settled_at", "provider_payout_id", "failure_reason"}

// phpayoutRow builds the FOR UPDATE payout row loadPayout scans.
func phpayoutRow(id, streamerID, bankAccountID uuid.UUID, amount int64, status string) *sqlmock.Rows {
	requestedAt := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	return sqlmock.NewRows(phpayoutCols).AddRow(
		id.String(), streamerID.String(), amount, status,
		nil, bankAccountID.String(), 1,
		requestedAt, nil, nil, nil, nil,
	)
}

// phmeta marshals a ledger_transactions metadata map the way the posting
// writers do (Go's json.Marshal sorts map keys, so bytes are exact).
func phmeta(kv map[string]string) []byte {
	b, err := json.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return b
}

// phquoted returns the regexp.QuoteMeta'd distinctive SQL prefix, matching
// the established idiom in this package.
func phquoted(s string) string { return regexp.QuoteMeta(s) }

// --- s3 reserve ------------------------------------------------------------

// TestPayoutRequestReservePostingsExactAmounts pins the request-side s3
// posting: PAYOUT_RESERVATION under op key create_payout:<id>, DEBIT
// STREAMER_AVAILABLE and CREDIT STREAMER_PAID, both STREAMER_PAID_DEBIT and
// exactly the payout amount. The deterministic D-051 payout id is asserted by
// expecting the derived id on every row.
func TestPayoutRequestReservePostingsExactAmounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	streamerID := uuid.New()
	bankID := uuid.New()
	requestID := uuid.New()
	const amount = int64(50000)
	const idempotencyKey = "ik-1"
	requestedAt := time.Date(2026, 10, 18, 10, 15, 0, 0, time.UTC)
	payoutID := uuid.MustParse(keys.PayoutRequestIDKey(streamerID.String(), idempotencyKey))
	availAcctID, paidAcctID := uuid.NewString(), uuid.NewString()

	// Fresh request replay-check: no row yet.
	mock.ExpectBegin()
	mock.ExpectQuery(phquoted("SELECT amount_idr,bank_account_id,status::text,requested_at FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(phquoted("SELECT bank_code,account_number_last4,is_verified FROM streamer_bank_accounts WHERE id=$1 AND streamer_id=$2 FOR UPDATE")).
		WithArgs(bankID.String(), streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"bank_code", "account_number_last4", "is_verified"}).AddRow("pivot", "1234", true))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE streamer_id=$1 AND status='ACTIVE' AND target_type IN ('STREAMER','PAYOUT'))")).
		WithArgs(streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type='STREAMER_AVAILABLE' AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(70000)))
	mock.ExpectQuery(phquoted("INSERT INTO payouts (id,streamer_id,amount_idr,bank_account_id) VALUES ($1,$2,$3,$4) RETURNING requested_at")).
		WithArgs(payoutID.String(), streamerID.String(), amount, bankID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"requested_at"}).AddRow(requestedAt))

	// s3 reserve transaction: op key create_payout:<id>, correlation =
	// request id, source_tag donation.
	opKey := keys.CreatePayoutKey(payoutID.String())
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_RESERVATION'")).
		WithArgs(sqlmock.AnyArg(), opKey, requestID.String(), streamerID.String(),
			phmeta(map[string]string{"idempotency_key": idempotencyKey, "source": "payout-command"}), ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))

	// Leg 1: DEBIT STREAMER_AVAILABLE, exactly the amount, STREAMER_PAID_DEBIT.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_AVAILABLE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(availAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), availAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, requestID.String(), sqlmock.AnyArg(), "{}").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(-amount, availAcctID).
		WillReturnResult(driver.RowsAffected(1))

	// Leg 2: CREDIT STREAMER_PAID, exactly the amount, STREAMER_PAID_DEBIT.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "CREDIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, requestID.String(), sqlmock.AnyArg(), "{}").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))

	// payout.requested.v1 in the outbox, same transaction.
	mock.ExpectExec(phquoted("INSERT INTO outbox_events (")).
		WithArgs(sqlmock.AnyArg(), "saruman", "payout.requested.v1", "1", "payout",
			payoutID.String(), streamerID.String(), "payout.requested.v1."+streamerID.String(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	result, err := CreatePayout(context.Background(), db, CreatePayoutRequest{
		StreamerID: streamerID, AmountIDR: amount, BankAccountID: bankID,
		IdempotencyKey: idempotencyKey, RequestID: requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PayoutID != payoutID {
		t.Fatalf("payout id = %s, want derived %s", result.PayoutID, payoutID)
	}
	if result.Status != PayoutRequested || result.AmountIDR != amount || result.Replay || !result.RequestedAt.Equal(requestedAt) {
		t.Fatalf("result = %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- s4 completion ---------------------------------------------------------

// TestPayoutCompletionPostingsExactAmounts pins the s4 completion (D-048):
// DEBIT STREAMER_PAID (STREAMER_PAID_DEBIT) + CREDIT DISBURSED
// (STREAMER_DISBURSED_CREDIT), exactly the payout amount, correlated to the
// completed event. The ordered expectations have no STREAMER_AVAILABLE touch
// anywhere in the completion transaction, so any availability drift fails
// this test: available aggregates are untouched across completion.
func TestPayoutCompletionPostingsExactAmounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()
	bankID := uuid.New()
	eventID := uuid.New()
	const amount = int64(50000)
	settledAt := time.Date(2026, 10, 18, 11, 30, 0, 0, time.UTC)
	paidAcctID, disbursedAcctID := uuid.NewString(), uuid.NewString()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, bankID, amount, PayoutProcessing))
	// Settle-time hold gate passes.
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))")).
		WithArgs(streamerID.String(), payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))

	// s4 transaction: op key complete_payout:<id>, correlation = event id.
	opKey := keys.CompletePayoutKey(payoutID.String())
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION'")).
		WithArgs(sqlmock.AnyArg(), opKey, eventID.String(), streamerID.String(),
			phmeta(map[string]string{"provider_payout_id": "pp-123", "source": events.PgGatewayWithdrawalCompleted}), ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))

	// Corruption paranoia: paid escrow must hold the full reserve.
	mock.ExpectQuery(phquoted("SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(999999)))

	// Leg 1: terminal DEBIT of paid escrow.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, eventID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(-amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))

	// Leg 2: CREDIT the DISBURSED account (amended s4, D-048).
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "DISBURSED").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(disbursedAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), disbursedAcctID, "CREDIT", amount, "STREAMER_DISBURSED_CREDIT",
			payoutID.String(), payoutID.String(), nil, eventID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(amount, disbursedAcctID).
		WillReturnResult(driver.RowsAffected(1))

	// Escrow must net to zero for this payout after the terminal debit.
	mock.ExpectQuery(phquoted("SELECT COALESCE(SUM(CASE WHEN e.direction='DEBIT' THEN -e.amount_idr ELSE e.amount_idr END),0) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID'")).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"net"}).AddRow(int64(0)))

	mock.ExpectExec(phquoted("UPDATE payouts SET status='SETTLED',settled_at=$2,provider_payout_id=$3,last_event_id=$4 WHERE id=$1 AND status='PROCESSING'")).
		WithArgs(payoutID.String(), settledAt, "pp-123", eventID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("INSERT INTO outbox_events (")).
		WithArgs(sqlmock.AnyArg(), "saruman", "payout.settled.v1", "1", "payout",
			payoutID.String(), streamerID.String(), "payout.settled.v1."+streamerID.String(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	out, err := WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID, ProviderPayoutID: "pp-123",
		BankAccountRef: "bank-ref", SettledAt: settledAt, EventID: eventID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutSettled || !out.Changed || !out.SettledAt.Equal(settledAt) {
		t.Fatalf("outcome = %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPayoutCompletionOnSettledRowIsStable: a second completed.v1 delivery
// against a SETTLED row is a stable no-op — no transaction, no postings.
func TestPayoutCompletionOnSettledRowIsStable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), 50000, PayoutSettled))
	mock.ExpectCommit()

	out, err := WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID, EventID: uuid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Changed || out.Status != "" {
		t.Fatalf("second delivery must be a stable no-op, got %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPayoutCompletionSkippedByOpKeyUniqueness models the second completed
// delivery arriving while the s4 completion row already exists (crash replay
// race): the complete_payout:<id> op key ON CONFLICT yields zero rows, the
// posting is skipped with no entry/balance work, and nothing is double-posted.
func TestPayoutCompletionSkippedByOpKeyUniqueness(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), 50000, PayoutProcessing))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))")).
		WithArgs(streamerID.String(), payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	// The op key already exists: ON CONFLICT DO NOTHING -> 0 rows -> skip.
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION'")).
		WithArgs(sqlmock.AnyArg(), keys.CompletePayoutKey(payoutID.String()), sqlmock.AnyArg(), streamerID.String(),
			sqlmock.AnyArg(), ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(0))
	mock.ExpectRollback()

	_, err = WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID, EventID: uuid.New(),
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate payout completion operation") {
		t.Fatalf("err = %v, want duplicate completion rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- s5 failure release ----------------------------------------------------

// TestPayoutFailureReleasePostingsExactAmounts pins the s5 release: the
// PAYOUT_FAILURE_RELEASE transaction links reversal_of to the s3 reservation
// transaction, the DEBIT leg links its reversal_of to the s3 paid-credit
// entry, CREDIT STREAMER_AVAILABLE restores exactly the payout amount
// (STREAMER_AVAILABLE_CREDIT) — net zero for the streamer — and the escrow
// invariant is re-checked.
func TestPayoutFailureReleasePostingsExactAmounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()
	bankID := uuid.New()
	eventID := uuid.New()
	reserveTxID := uuid.New()
	reservePaidEntryID := uuid.New()
	const amount = int64(50000)
	failedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	paidAcctID, availAcctID := uuid.NewString(), uuid.NewString()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, bankID, amount, PayoutProcessing))

	// Reversal linkage sources: the s3 reservation transaction and its
	// paid-credit entry.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_transactions WHERE operation_key=$1")).
		WithArgs(keys.CreatePayoutKey(payoutID.String())).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(reserveTxID.String()))
	mock.ExpectQuery(phquoted("SELECT e.id FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID' AND e.entry_type='STREAMER_PAID_DEBIT' ORDER BY e.created_at LIMIT 1")).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(reservePaidEntryID.String()))

	// s5 transaction: op key fail_payout:<id>, reversal_of -> reserve tx.
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,reversal_of,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_FAILURE_RELEASE'")).
		WithArgs(sqlmock.AnyArg(), keys.FailPayoutKey(payoutID.String()), eventID.String(), streamerID.String(),
			reserveTxID.String(),
			phmeta(map[string]string{"provider_error": "bank_rejected", "source": events.PgGatewayWithdrawalFailed}),
			ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))

	// Leg 1: DEBIT STREAMER_PAID whose entry reversal_of links the s3
	// paid-credit entry at both levels.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), reservePaidEntryID.String(), eventID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(-amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))

	// Leg 2: CREDIT STREAMER_AVAILABLE restores exactly the amount.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_AVAILABLE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(availAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), availAcctID, "CREDIT", amount, "STREAMER_AVAILABLE_CREDIT",
			payoutID.String(), payoutID.String(), nil, eventID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(amount, availAcctID).
		WillReturnResult(driver.RowsAffected(1))

	mock.ExpectQuery(phquoted("SELECT COALESCE(SUM(CASE WHEN e.direction='DEBIT' THEN -e.amount_idr ELSE e.amount_idr END),0) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID'")).
		WithArgs(payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"net"}).AddRow(int64(0)))

	mock.ExpectExec(phquoted("UPDATE payouts SET status='FAILED',failure_reason=$2,last_event_id=$3 WHERE id=$1 AND status='PROCESSING'")).
		WithArgs(payoutID.String(), "PROVIDER_DECLINED", eventID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("INSERT INTO outbox_events (")).
		WithArgs(sqlmock.AnyArg(), "saruman", "payout.failed.v1", "1", "payout",
			payoutID.String(), streamerID.String(), "payout.failed.v1."+streamerID.String(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	out, err := WithdrawalFailed(context.Background(), db, WithdrawalFailedInput{
		PayoutID: payoutID, StreamerID: streamerID, FailureReason: "PROVIDER_DECLINED",
		ProviderError: "bank_rejected", FailedAt: failedAt, EventID: eventID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutFailed || !out.Changed {
		t.Fatalf("outcome = %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- account-hierarchy enforcement ----------------------------------------

// TestPayoutCompletionRejectsMissingReserve: without the s3 reserve the paid
// escrow cannot cover the terminal debit — the completion is structurally
// rejected with the escrow-short error and the transaction rolls back, so no
// partial posting can ever survive.
func TestPayoutCompletionRejectsMissingReserve(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), 50000, PayoutProcessing))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))")).
		WithArgs(streamerID.String(), payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION'")).
		WithArgs(sqlmock.AnyArg(), keys.CompletePayoutKey(payoutID.String()), sqlmock.AnyArg(), streamerID.String(),
			sqlmock.AnyArg(), ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))
	// Paid escrow holds 0: the reserve posting is absent.
	mock.ExpectQuery(phquoted("SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(0)))
	mock.ExpectRollback()

	_, err = WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID, EventID: uuid.New(),
	})
	if err == nil || !regexp.MustCompile(`STREAMER_PAID escrow short \(balance=0 need=50000\)`).MatchString(err.Error()) {
		t.Fatalf("err = %v, want escrow-short rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPayoutCompletionRejectsUnavailableDisbursedAccount: the DISBURSED
// account row being absent is a structural failure — the completion errors
// out before any DISBURSED balance write, and the transaction rolls back.
func TestPayoutCompletionRejectsUnavailableDisbursedAccount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()
	const amount = int64(50000)
	paidAcctID := uuid.NewString()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), amount, PayoutProcessing))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))")).
		WithArgs(streamerID.String(), payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION'")).
		WithArgs(sqlmock.AnyArg(), keys.CompletePayoutKey(payoutID.String()), sqlmock.AnyArg(), streamerID.String(),
			sqlmock.AnyArg(), ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(999999)))
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(-amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))
	// DISBURSED account row does not exist.
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "DISBURSED").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID, EventID: uuid.New(),
	})
	if err == nil || !errors.Is(err, sql.ErrNoRows) || !regexp.MustCompile(`ledger account DISBURSED`).MatchString(err.Error()) {
		t.Fatalf("err = %v, want structural DISBURSED rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPayoutReleaseRejectsMissingReservationTx: a failure release whose s3
// reservation transaction is absent is rejected before any posting — the
// money-model hierarchy (reserve -> terminal) is enforced structurally.
func TestPayoutReleaseRejectsMissingReservationTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), 50000, PayoutProcessing))
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_transactions WHERE operation_key=$1")).
		WithArgs(keys.CreatePayoutKey(payoutID.String())).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = WithdrawalFailed(context.Background(), db, WithdrawalFailedInput{
		PayoutID: payoutID, StreamerID: streamerID, EventID: uuid.New(),
	})
	if err == nil || !regexp.MustCompile(`reservation transaction missing for release`).MatchString(err.Error()) {
		t.Fatalf("err = %v, want missing-reservation rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- nullability -----------------------------------------------------------

// TestPayoutPostingCorrelationNullability pins the toNullUUID contract: when
// the provider event carries no correlation/event id, the completion and
// failure transactions and their entries carry SQL NULL correlation columns,
// never an all-zero uuid alias.
func TestPayoutPostingCorrelationNullability(t *testing.T) {
	t.Run("completion with no event id posts NULL correlation", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(true)

		payoutID := uuid.New()
		streamerID := uuid.New()
		const amount = int64(50000)
		paidAcctID, disbursedAcctID := uuid.NewString(), uuid.NewString()

		mock.ExpectBegin()
		mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
			WithArgs(payoutID.String()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
			WithArgs(payoutID.String()).
			WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), amount, PayoutProcessing))
		mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))")).
			WithArgs(streamerID.String(), payoutID.String()).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
			WithArgs(streamerID.String()).
			WillReturnResult(driver.RowsAffected(1))
		// $3 = correlation_id must be NULL (toNullUUID(uuid.Nil)).
		mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION'")).
			WithArgs(sqlmock.AnyArg(), keys.CompletePayoutKey(payoutID.String()), nil, streamerID.String(),
				phmeta(map[string]string{"source": events.PgGatewayWithdrawalCompleted}), ledger.SourceDonation).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
			WithArgs(streamerID.String(), "STREAMER_PAID").
			WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(999999)))
		mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
			WithArgs(streamerID.String(), "STREAMER_PAID").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
		mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
			WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
				payoutID.String(), payoutID.String(), nil, nil, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
			WithArgs(-amount, paidAcctID).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
			WithArgs(streamerID.String(), "DISBURSED").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(disbursedAcctID))
		mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
			WithArgs(sqlmock.AnyArg(), streamerID.String(), disbursedAcctID, "CREDIT", amount, "STREAMER_DISBURSED_CREDIT",
				payoutID.String(), payoutID.String(), nil, nil, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
			WithArgs(amount, disbursedAcctID).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT COALESCE(SUM(CASE WHEN e.direction='DEBIT' THEN -e.amount_idr ELSE e.amount_idr END),0) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID'")).
			WithArgs(payoutID.String()).
			WillReturnRows(sqlmock.NewRows([]string{"net"}).AddRow(int64(0)))
		mock.ExpectExec(phquoted("UPDATE payouts SET status='SETTLED',settled_at=$2,provider_payout_id=$3,last_event_id=$4 WHERE id=$1 AND status='PROCESSING'")).
			WithArgs(payoutID.String(), sqlmock.AnyArg(), "", nil).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(phquoted("INSERT INTO outbox_events (")).
			WithArgs(sqlmock.AnyArg(), "saruman", "payout.settled.v1", "1", "payout",
				payoutID.String(), streamerID.String(), "payout.settled.v1."+streamerID.String(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectCommit()

		out, err := WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
			PayoutID: payoutID, StreamerID: streamerID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if out.Status != PayoutSettled || !out.Changed {
			t.Fatalf("outcome = %+v", out)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failure with no event id posts NULL correlation with non-null reversal", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(true)

		payoutID := uuid.New()
		streamerID := uuid.New()
		reserveTxID := uuid.New()
		reservePaidEntryID := uuid.New()
		const amount = int64(50000)
		paidAcctID, availAcctID := uuid.NewString(), uuid.NewString()

		mock.ExpectBegin()
		mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
			WithArgs(payoutID.String()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
			WithArgs(payoutID.String()).
			WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), amount, PayoutProcessing))
		mock.ExpectQuery(phquoted("SELECT id FROM ledger_transactions WHERE operation_key=$1")).
			WithArgs(keys.CreatePayoutKey(payoutID.String())).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(reserveTxID.String()))
		mock.ExpectQuery(phquoted("SELECT e.id FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID' AND e.entry_type='STREAMER_PAID_DEBIT' ORDER BY e.created_at LIMIT 1")).
			WithArgs(payoutID.String()).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(reservePaidEntryID.String()))
		// correlation ($3) NULL; reversal_of ($5) links the reserve tx.
		mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,reversal_of,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_FAILURE_RELEASE'")).
			WithArgs(sqlmock.AnyArg(), keys.FailPayoutKey(payoutID.String()), nil, streamerID.String(),
				reserveTxID.String(),
				phmeta(map[string]string{"source": events.PgGatewayWithdrawalFailed}),
				ledger.SourceDonation).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
			WithArgs(streamerID.String(), "STREAMER_PAID").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
		mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
			WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
				payoutID.String(), payoutID.String(), reservePaidEntryID.String(), nil, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
			WithArgs(-amount, paidAcctID).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
			WithArgs(streamerID.String(), "STREAMER_AVAILABLE").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(availAcctID))
		mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
			WithArgs(sqlmock.AnyArg(), streamerID.String(), availAcctID, "CREDIT", amount, "STREAMER_AVAILABLE_CREDIT",
				payoutID.String(), payoutID.String(), nil, nil, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
			WithArgs(amount, availAcctID).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectQuery(phquoted("SELECT COALESCE(SUM(CASE WHEN e.direction='DEBIT' THEN -e.amount_idr ELSE e.amount_idr END),0) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID'")).
			WithArgs(payoutID.String()).
			WillReturnRows(sqlmock.NewRows([]string{"net"}).AddRow(int64(0)))
		mock.ExpectExec(phquoted("UPDATE payouts SET status='FAILED',failure_reason=$2,last_event_id=$3 WHERE id=$1 AND status='PROCESSING'")).
			WithArgs(payoutID.String(), "PROVIDER_FAILURE", nil).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(phquoted("INSERT INTO outbox_events (")).
			WithArgs(sqlmock.AnyArg(), "saruman", "payout.failed.v1", "1", "payout",
				payoutID.String(), streamerID.String(), "payout.failed.v1."+streamerID.String(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(driver.RowsAffected(1))
		mock.ExpectCommit()

		out, err := WithdrawalFailed(context.Background(), db, WithdrawalFailedInput{
			PayoutID: payoutID, StreamerID: streamerID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if out.Status != PayoutFailed || !out.Changed {
			t.Fatalf("outcome = %+v", out)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

// --- holds -----------------------------------------------------------------

// TestPayoutCreateHoldRaceCaughtBySerializableRetry models the hold TOCTOU: a
// fund_holds row lands between the reserve check and commit, so the first
// transaction aborts at COMMIT with SQLSTATE 40001. runSerializable retries;
// on the retried pass the ACTIVE hold is visible and the reservation is
// rejected with ErrPayoutHoldActive — no money moves on either attempt.
func TestPayoutCreateHoldRaceCaughtBySerializableRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	streamerID := uuid.New()
	bankID := uuid.New()
	requestID := uuid.New()
	const amount = int64(50000)
	const idempotencyKey = "ik-toctou"
	requestedAt := time.Date(2026, 10, 18, 10, 15, 0, 0, time.UTC)
	payoutID := uuid.MustParse(keys.PayoutRequestIDKey(streamerID.String(), idempotencyKey))
	availAcctID, paidAcctID := uuid.NewString(), uuid.NewString()

	// Attempt 1: passes every check, then COMMIT aborts on a serialization
	// failure (the concurrent hold insert broke isolation).
	mock.ExpectBegin()
	mock.ExpectQuery(phquoted("SELECT amount_idr,bank_account_id,status::text,requested_at FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(phquoted("SELECT bank_code,account_number_last4,is_verified FROM streamer_bank_accounts WHERE id=$1 AND streamer_id=$2 FOR UPDATE")).
		WithArgs(bankID.String(), streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"bank_code", "account_number_last4", "is_verified"}).AddRow("pivot", "1234", true))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE streamer_id=$1 AND status='ACTIVE' AND target_type IN ('STREAMER','PAYOUT'))")).
		WithArgs(streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(phquoted("INSERT INTO ledger_accounts (streamer_id,account_type,currency)")).
		WithArgs(streamerID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type='STREAMER_AVAILABLE' AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_idr"}).AddRow(int64(70000)))
	mock.ExpectQuery(phquoted("INSERT INTO payouts (id,streamer_id,amount_idr,bank_account_id) VALUES ($1,$2,$3,$4) RETURNING requested_at")).
		WithArgs(payoutID.String(), streamerID.String(), amount, bankID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"requested_at"}).AddRow(requestedAt))
	mock.ExpectExec(phquoted("INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_RESERVATION'")).
		WithArgs(sqlmock.AnyArg(), keys.CreatePayoutKey(payoutID.String()), requestID.String(), streamerID.String(),
			phmeta(map[string]string{"idempotency_key": idempotencyKey, "source": "payout-command"}), ledger.SourceDonation).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_AVAILABLE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(availAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), availAcctID, "DEBIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, requestID.String(), sqlmock.AnyArg(), "{}").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(-amount, availAcctID).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE")).
		WithArgs(streamerID.String(), "STREAMER_PAID").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(paidAcctID))
	mock.ExpectExec(phquoted("INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES")).
		WithArgs(sqlmock.AnyArg(), streamerID.String(), paidAcctID, "CREDIT", amount, "STREAMER_PAID_DEBIT",
			payoutID.String(), payoutID.String(), nil, requestID.String(), sqlmock.AnyArg(), "{}").
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2")).
		WithArgs(amount, paidAcctID).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(phquoted("INSERT INTO outbox_events (")).
		WithArgs(sqlmock.AnyArg(), "saruman", "payout.requested.v1", "1", "payout",
			payoutID.String(), streamerID.String(), "payout.requested.v1."+streamerID.String(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit().WillReturnError(&pgconn.PgError{Code: "40001", Message: "could not serialize access due to concurrent update", Severity: "ERROR"})

	// Attempt 2: the retry now observes the ACTIVE hold that slipped in
	// between the first check and commit -> rejection, nothing ever posted.
	mock.ExpectBegin()
	mock.ExpectQuery(phquoted("SELECT amount_idr,bank_account_id,status::text,requested_at FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(phquoted("SELECT bank_code,account_number_last4,is_verified FROM streamer_bank_accounts WHERE id=$1 AND streamer_id=$2 FOR UPDATE")).
		WithArgs(bankID.String(), streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"bank_code", "account_number_last4", "is_verified"}).AddRow("pivot", "1234", true))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE streamer_id=$1 AND status='ACTIVE' AND target_type IN ('STREAMER','PAYOUT'))")).
		WithArgs(streamerID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	result, err := CreatePayout(context.Background(), db, CreatePayoutRequest{
		StreamerID: streamerID, AmountIDR: amount, BankAccountID: bankID,
		IdempotencyKey: idempotencyKey, RequestID: requestID,
	})
	if !errors.Is(err, ErrPayoutHoldActive) {
		t.Fatalf("err = %v, want ErrPayoutHoldActive from the retried pass", err)
	}
	if result.Replay {
		t.Fatalf("result = %+v, want zero-value after rejection", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPayoutSettleTimeHoldGateBlocksCompletion models D-056: a hold created
// between submit and completion (the payout is PROCESSING) gates settlement —
// the payout parks in NEEDS_REVIEW with HOLD_CONFLICT and the s4 completion
// is never posted (the ordered mock has no ledger-transaction expectation
// after the hold verdict).
func TestPayoutSettleTimeHoldGateBlocksCompletion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)

	payoutID := uuid.New()
	streamerID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(phquoted("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(phquoted("SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE")).
		WithArgs(payoutID.String()).
		WillReturnRows(phpayoutRow(payoutID, streamerID, uuid.New(), 50000, PayoutProcessing))
	mock.ExpectQuery(phquoted("SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))")).
		WithArgs(streamerID.String(), payoutID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(phquoted("UPDATE payouts SET status='NEEDS_REVIEW',failure_reason='HOLD_CONFLICT' WHERE id=$1")).
		WithArgs(payoutID.String()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	out, err := WithdrawalCompleted(context.Background(), db, WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID, EventID: uuid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != PayoutNeedsReview || !out.Changed {
		t.Fatalf("outcome = %+v, want NEEDS_REVIEW hold gate", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
