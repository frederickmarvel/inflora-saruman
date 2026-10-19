// Package service implements the payout state machine (CONTRACT_DRIFT
// D-047..D-058). Saruman owns every money effect on payouts: the request
// path reserves available funds into paid-escrow (s3), the withdrawal-completed
// path settles them into the DISBURSED account (s4, D-048), and the
// withdrawal-failed path releases them back to available (s5). The provider
// attempt itself happens at REQUESTED inside Palantir (D-055); Submit* here is
// bookkeeping plus a lease raise plus a gateway-row assert.
//
// Posting templates: almanac/planning/schemas/10-posting-templates.md §3-§5.
package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
)

// Payout statuses. NEEDS_REVIEW is the provider-lease sweep terminal
// (D-057): reserved money stays in paid-escrow until an operator resolves.
const (
	PayoutRequested   = "REQUESTED"
	PayoutBatched     = "BATCHED"
	PayoutProcessing  = "PROCESSING"
	PayoutSettled     = "SETTLED"
	PayoutFailed      = "FAILED"
	PayoutNeedsReview = "NEEDS_REVIEW"
)

// MinPayoutIDR mirrors the chk_min_withdrawal check on payouts.
const MinPayoutIDR = 10000

// DefaultPayoutLease is the provider-attempt liveness window; the sweep flags
// rows whose reservation/lease predates twice this window (D-057).
const DefaultPayoutLease = 15 * time.Minute

// transition admits exactly the registered transitions from the Phase C
// design (T7/D-055/D-057): REQUESTED -> {BATCHED, PROCESSING, NEEDS_REVIEW},
// BATCHED -> {REQUESTED, PROCESSING, NEEDS_REVIEW}, PROCESSING -> {SETTLED,
// FAILED, NEEDS_REVIEW}, NEEDS_REVIEW -> {PROCESSING, SETTLED, FAILED}.
// Settled and failed are terminal. BATCHED -> NEEDS_REVIEW is the
// provider-event park registered by CONTRACT_DRIFT D-059 (the member is
// detached from its batch, so a late DRAFT cancel never trips the FK). No
// other transition is registered.
var payoutTransitions = map[string]map[string]bool{
	PayoutRequested:   {PayoutBatched: true, PayoutProcessing: true, PayoutNeedsReview: true},
	PayoutBatched:     {PayoutRequested: true, PayoutProcessing: true, PayoutNeedsReview: true},
	PayoutProcessing:  {PayoutSettled: true, PayoutFailed: true, PayoutNeedsReview: true},
	PayoutNeedsReview: {PayoutProcessing: true, PayoutSettled: true, PayoutFailed: true},
	PayoutSettled:     {},
	PayoutFailed:      {},
}

// CanTransition reports whether from -> to is a registered payout transition.
func CanTransition(from, to string) bool { return payoutTransitions[from][to] }

// Payout service errors; the HTTP layer maps them to the frozen codes
// (400/403/404/409/422 per D-053/D-055).
var (
	ErrPayoutNotFound            = errors.New("payout: not found")
	ErrPayoutValidation          = errors.New("payout: invalid request")
	ErrPayoutBelowMinimum        = errors.New("payout: below minimum withdrawal")
	ErrPayoutIllegalTransition   = errors.New("payout: illegal transition")
	ErrPayoutInsufficientBalance = errors.New("payout: insufficient available balance")
	ErrPayoutHoldActive          = errors.New("payout: streamer hold active")
	ErrPayoutBankNotFound        = errors.New("payout: bank account not found")
	ErrPayoutBankNotVerified     = errors.New("payout: bank account not verified")
	ErrPayoutIdempotencyConflict = errors.New("payout: idempotency conflict")
	ErrPayoutGatewayPending      = errors.New("payout: gateway withdrawal row missing")
	ErrPayoutBatchNotFound       = errors.New("payout: batch not found")
	ErrPayoutBatchIllegalState   = errors.New("payout: batch illegal state")
)

// CreatePayoutRequest is the withdrawal-eligibility input from the facade.
type CreatePayoutRequest struct {
	StreamerID     uuid.UUID
	AmountIDR      int64
	BankAccountID  uuid.UUID
	IdempotencyKey string    // optional; derives the payout id (D-051)
	RequestID      uuid.UUID // facade request id; becomes the s3 correlation
}

// CreatePayoutResult reports the reservation outcome. Replay is true when a
// previous identical request created the row.
type CreatePayoutResult struct {
	PayoutID    uuid.UUID
	AmountIDR   int64
	Status      string
	RequestedAt time.Time
	Replay      bool
}

// payoutRow is the CAS view of a payout the transition paths need.
type payoutRow struct {
	ID            uuid.UUID
	StreamerID    uuid.UUID
	AmountIDR     int64
	Status        string
	BatchID       uuid.NullUUID
	BankAccountID uuid.UUID
	Attempts      int
	RequestedAt   time.Time
	ProcessedAt   sql.NullTime
	SettledAt     sql.NullTime
	ProviderPayoutID sql.NullString
	FailureReason sql.NullString
}

func loadPayout(ctx context.Context, tx *sql.Tx, id uuid.UUID) (payoutRow, error) {
	var p payoutRow
	err := tx.QueryRowContext(ctx, `SELECT id,streamer_id,amount_idr,status::text,batch_id,bank_account_id,attempts,requested_at,processed_at,settled_at,provider_payout_id,failure_reason FROM payouts WHERE id=$1 FOR UPDATE`, id).
		Scan(&p.ID, &p.StreamerID, &p.AmountIDR, &p.Status, &p.BatchID, &p.BankAccountID, &p.Attempts, &p.RequestedAt, &p.ProcessedAt, &p.SettledAt, &p.ProviderPayoutID, &p.FailureReason)
	return p, err
}

// claimSequentially serializes concurrent transition attempts on one payout.
func claimSequentially(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	_, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", id.String())
	return err
}

// runSerializable executes fn inside a serializable transaction, retrying on
// serialization failures (SQLSTATE 40001) and unique violations (23505 —
// a deterministic-id insert racing its twin). Fn must be idempotent.
func runSerializable(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	for attempt := 0; ; attempt++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			if err = tx.Commit(); err == nil {
				return nil
			}
		} else {
			_ = tx.Rollback()
		}
		var retryable bool
		if pgErr, ok := errToPG(err); ok {
			retryable = pgErr.Code == "40001" || pgErr.Code == "23505"
		}
		if !retryable || attempt >= 3 {
			return err
		}
		// Serialization failures settle quickly; a small backoff avoids a
		// hot retry loop in a thundering-herd settlement window.
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
}

// CreatePayout reserves streamer funds for a withdrawal (s3). The
// eligibility check and the ledger insert share one serializable transaction
// so two concurrent requests cannot both pass the balance check (frozen
// template §3). A replayed request with the same Idempotency-Key returns the
// existing row; a key reuse with different amount/bank is a conflict.
func CreatePayout(ctx context.Context, db *sql.DB, req CreatePayoutRequest) (CreatePayoutResult, error) {
	if req.AmountIDR < MinPayoutIDR {
		return CreatePayoutResult{}, ErrPayoutBelowMinimum
	}
	if req.StreamerID == uuid.Nil || req.BankAccountID == uuid.Nil {
		return CreatePayoutResult{}, ErrPayoutValidation
	}
	if req.RequestID == uuid.Nil {
		req.RequestID = uuid.New()
	}
	payoutID := uuid.New()
	if req.IdempotencyKey != "" {
		payoutID = uuid.MustParse(keys.PayoutRequestIDKey(req.StreamerID.String(), req.IdempotencyKey))
	}
	result := CreatePayoutResult{}
	err := runSerializable(ctx, db, func(tx *sql.Tx) error {
		// Replay: the deterministic id pins the row; a key reuse with a
		// different payload is a conflict (D-051).
		var existingAmount int64
		var existingBank, existingStatus string
		var existingAt time.Time
		err := tx.QueryRowContext(ctx, `SELECT amount_idr,bank_account_id,status::text,requested_at FROM payouts WHERE id=$1 FOR UPDATE`, payoutID).
			Scan(&existingAmount, &existingBank, &existingStatus, &existingAt)
		switch {
		case err == nil:
			if existingAmount != req.AmountIDR || existingBank != req.BankAccountID.String() {
				return ErrPayoutIdempotencyConflict
			}
			result = CreatePayoutResult{PayoutID: payoutID, AmountIDR: existingAmount, Status: existingStatus, RequestedAt: existingAt, Replay: true}
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("payout: check replay: %w", err)
		}

		// Bank ownership + verification.
		var bankCode, last4 string
		var verified bool
		err = tx.QueryRowContext(ctx, `SELECT bank_code,account_number_last4,is_verified FROM streamer_bank_accounts WHERE id=$1 AND streamer_id=$2 FOR UPDATE`, req.BankAccountID, req.StreamerID).
			Scan(&bankCode, &last4, &verified)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutBankNotFound
		}
		if err != nil {
			return fmt.Errorf("payout: load bank: %w", err)
		}
		if !verified {
			return ErrPayoutBankNotVerified
		}

		// Active streamer/payout-scoped holds block new reservations.
		var held bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fund_holds WHERE streamer_id=$1 AND status='ACTIVE' AND target_type IN ('STREAMER','PAYOUT'))`, req.StreamerID).Scan(&held); err != nil {
			return fmt.Errorf("payout: check holds: %w", err)
		}
		if held {
			return ErrPayoutHoldActive
		}

		if err := ensurePayoutAccounts(ctx, tx, req.StreamerID); err != nil {
			return err
		}
		var balance int64
		if err := tx.QueryRowContext(ctx, `SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type='STREAMER_AVAILABLE' AND currency='IDR' FOR UPDATE`, req.StreamerID).Scan(&balance); err != nil {
			return fmt.Errorf("payout: load available balance: %w", err)
		}
		if balance < req.AmountIDR {
			return ErrPayoutInsufficientBalance
		}

		var requestedAt time.Time
		if err := tx.QueryRowContext(ctx, `INSERT INTO payouts (id,streamer_id,amount_idr,bank_account_id) VALUES ($1,$2,$3,$4) RETURNING requested_at`, payoutID, req.StreamerID, req.AmountIDR, req.BankAccountID).Scan(&requestedAt); err != nil {
			return fmt.Errorf("payout: insert row: %w", err)
		}
		if err := postReserve(ctx, tx, payoutID, req.StreamerID, req.AmountIDR, req.RequestID, req.IdempotencyKey); err != nil {
			return err
		}
		payload := events.PayoutRequestedEvent{
			PayoutID: payoutID.String(), StreamerID: req.StreamerID.String(),
			AmountIDR: req.AmountIDR, BankAccountID: req.BankAccountID.String(),
			BankAccountLast4: last4, BankCode: bankCode, RequestedAt: requestedAt,
		}
		if err := enqueuePayoutEvent(ctx, tx, events.PayoutRequested, payoutID, req.StreamerID, payload); err != nil {
			return err
		}
		result = CreatePayoutResult{PayoutID: payoutID, AmountIDR: req.AmountIDR, Status: PayoutRequested, RequestedAt: requestedAt}
		return nil
	})
	return result, err
}

// SubmitPayout moves a REQUESTED payout to PROCESSING: attempt counter, lease
// raise, and the gateway-row assert (D-055 — the provider call itself already
// happened at REQUESTED inside Palantir). Re-submission of an already
// PROCESSING row refreshes the lease. BATCHED payouts submit via SubmitBatch.
func SubmitPayout(ctx context.Context, db *sql.DB, payoutID uuid.UUID, lease time.Duration) (attempts int, leaseExpiresAt time.Time, err error) {
	if lease <= 0 {
		lease = DefaultPayoutLease
	}
	err = runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, payoutID); err != nil {
			return err
		}
		p, err := loadPayout(ctx, tx, payoutID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutNotFound
		}
		if err != nil {
			return err
		}
		switch p.Status {
		case PayoutRequested, PayoutProcessing:
		default:
			return ErrPayoutIllegalTransition
		}
		var gwExists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM gateway_withdrawals WHERE saruman_payout_id=$1)`, payoutID).Scan(&gwExists); err != nil {
			return err
		}
		if !gwExists {
			return ErrPayoutGatewayPending
		}
		expires := time.Now().UTC().Add(lease)
		if err := tx.QueryRowContext(ctx, `UPDATE payouts SET status='PROCESSING',attempts=attempts+1,processed_at=COALESCE(processed_at,NOW()),lease_expires_at=$2 WHERE id=$1 RETURNING attempts,lease_expires_at`, payoutID, expires).Scan(&attempts, &leaseExpiresAt); err != nil {
			return err
		}
		return nil
	})
	return attempts, leaseExpiresAt, err
}

// WithdrawalCompletedInput carries the pg.gateway.withdrawal.completed.v1
// payload plus the inbox event id (correlation for the s4 transaction).
type WithdrawalCompletedInput struct {
	PayoutID         uuid.UUID
	StreamerID       uuid.UUID
	ProviderPayoutID string
	BankAccountRef   string
	SettledAt        time.Time
	EventID          uuid.UUID
	CorrelationID    uuid.UUID
}

// CompletionOutcome reports what the completed-event handler did.
type CompletionOutcome struct {
	PayoutID  uuid.UUID
	Status    string
	SettledAt time.Time
	Changed   bool
}

// WithdrawalCompleted settles a PROCESSING payout (s4): the settle-time hold
// gate re-checks ACTIVE STREAMER/PAYOUT holds in the same serializable
// transaction (D-056) — a conflict parks the payout in NEEDS_REVIEW with
// HOLD_CONFLICT and the money stays reserved. A completed event on a
// REQUESTED/BATCHED row parks it in NEEDS_REVIEW (no fast path, D-055).
// Late duplicates on settled rows are idempotent; terminal rows never move.
func WithdrawalCompleted(ctx context.Context, db *sql.DB, in WithdrawalCompletedInput) (CompletionOutcome, error) {
	out := CompletionOutcome{PayoutID: in.PayoutID}
	err := runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, in.PayoutID); err != nil {
			return err
		}
		p, err := loadPayout(ctx, tx, in.PayoutID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutNotFound
		}
		if err != nil {
			return err
		}
		if p.StreamerID != in.StreamerID {
			return ErrPayoutNotFound
		}
		switch p.Status {
		case PayoutSettled:
			return nil // late duplicate: stable, idempotent
		case PayoutFailed, PayoutNeedsReview:
			return nil // terminal outcome already recorded; never regress
		case PayoutRequested, PayoutBatched:
			// Provider completed before any submit bookkeeping: park for
			// operator resolution; rare, reserved money stays escrowed. A
			// BATCHED member is detached so DRAFT-cancel keeps working and
			// batch counters stay truthful (CONTRACT_DRIFT D-059).
			if p.Status == PayoutBatched {
				if _, err := tx.ExecContext(ctx, `UPDATE payout_batches SET total_amount_idr=total_amount_idr-$1,payout_count=payout_count-1 WHERE id=$2`, p.AmountIDR, p.BatchID.UUID); err != nil {
					return err
				}
				res, err := tx.ExecContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2,batch_id=NULL WHERE id=$1`, p.ID, "PROVIDER_COMPLETED_BEFORE_SUBMIT")
				out.Status = PayoutNeedsReview
				out.Changed = res != nil
				return err
			}
			res, err := tx.ExecContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2 WHERE id=$1`, p.ID, "PROVIDER_COMPLETED_BEFORE_SUBMIT")
			out.Status = PayoutNeedsReview
			out.Changed = res != nil
			return err
		case PayoutProcessing:
			// Settle-time hold gate (D-056): holds placed after request but
			// before completion must stop settlement.
			var held bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fund_holds WHERE status='ACTIVE' AND ((target_type='STREAMER' AND streamer_id=$1) OR (target_type='PAYOUT' AND payout_id=$2)))`, p.StreamerID, p.ID).Scan(&held); err != nil {
				return err
			}
			if held {
				if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW',failure_reason='HOLD_CONFLICT' WHERE id=$1`, p.ID); err != nil {
					return err
				}
				out.Status = PayoutNeedsReview
				out.Changed = true
				return nil
			}
			if err := ensurePayoutAccounts(ctx, tx, p.StreamerID); err != nil {
				return err
			}
			if err := postComplete(ctx, tx, p, in); err != nil {
				return err
			}
			settledAt := in.SettledAt
			if settledAt.IsZero() {
				settledAt = time.Now().UTC()
			}
			res, err := tx.ExecContext(ctx, `UPDATE payouts SET status='SETTLED',settled_at=$2,provider_payout_id=$3,last_event_id=$4 WHERE id=$1 AND status='PROCESSING'`, p.ID, settledAt, in.ProviderPayoutID, toNullUUID(in.EventID))
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return ErrPayoutIllegalTransition
			}
			payload := events.PayoutSettledEvent{
				PayoutID: p.ID.String(), StreamerID: p.StreamerID.String(),
				AmountIDR: p.AmountIDR, ProviderPayoutID: in.ProviderPayoutID,
				BankReference: in.BankAccountRef, SettledAt: settledAt,
			}
			if err := enqueuePayoutEvent(ctx, tx, events.PayoutSettled, p.ID, p.StreamerID, payload); err != nil {
				return err
			}
			if err := deriveBatch(ctx, tx, p.BatchID); err != nil {
				return err
			}
			out.Status = PayoutSettled
			out.SettledAt = settledAt
			out.Changed = true
			return nil
		}
		return ErrPayoutIllegalTransition
	})
	if err != nil {
		return CompletionOutcome{}, err
	}
	return out, nil
}

// WithdrawalFailedInput carries pg.gateway.withdrawal.failed.v1.
type WithdrawalFailedInput struct {
	PayoutID      uuid.UUID
	StreamerID    uuid.UUID
	FailureReason string
	ProviderError string
	FailedAt      time.Time
	EventID       uuid.UUID
	CorrelationID uuid.UUID
}

// FailureOutcome reports what the failed-event handler did.
type FailureOutcome struct {
	PayoutID uuid.UUID
	Status   string
	Changed  bool
}

// WithdrawalFailed fails a PROCESSING payout and releases the s3 reserve
// (s5). A failed event on a REQUESTED/BATCHED row parks it in NEEDS_REVIEW
// with the provider error and no release (D-055 — the attempt failed at
// request time; only an operator releases). Terminal rows never move.
func WithdrawalFailed(ctx context.Context, db *sql.DB, in WithdrawalFailedInput) (FailureOutcome, error) {
	out := FailureOutcome{PayoutID: in.PayoutID}
	err := runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, in.PayoutID); err != nil {
			return err
		}
		p, err := loadPayout(ctx, tx, in.PayoutID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutNotFound
		}
		if err != nil {
			return err
		}
		if p.StreamerID != in.StreamerID {
			return ErrPayoutNotFound
		}
		switch p.Status {
		case PayoutSettled, PayoutFailed, PayoutNeedsReview:
			return nil // late duplicate or already-reviewed: stable
		case PayoutRequested, PayoutBatched:
			reason := surfaceSafeReason(in.FailureReason, in.ProviderError)
			// A BATCHED member is detached before the park so DRAFT-cancel
			// keeps working and batch counters stay truthful (D-059).
			if p.Status == PayoutBatched {
				if _, err := tx.ExecContext(ctx, `UPDATE payout_batches SET total_amount_idr=total_amount_idr-$1,payout_count=payout_count-1 WHERE id=$2`, p.AmountIDR, p.BatchID.UUID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2,batch_id=NULL WHERE id=$1`, p.ID, reason); err != nil {
					return err
				}
				out.Status = PayoutNeedsReview
				out.Changed = true
				return nil
			}
			if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2 WHERE id=$1`, p.ID, reason); err != nil {
				return err
			}
			out.Status = PayoutNeedsReview
			out.Changed = true
			return nil
		case PayoutProcessing:
			reason := surfaceSafeReason(in.FailureReason, "")
			if err := postRelease(ctx, tx, p.ID, p.StreamerID, p.AmountIDR, in); err != nil {
				return err
			}
			failedAt := in.FailedAt
			if failedAt.IsZero() {
				failedAt = time.Now().UTC()
			}
			res, err := tx.ExecContext(ctx, `UPDATE payouts SET status='FAILED',failure_reason=$2,last_event_id=$3 WHERE id=$1 AND status='PROCESSING'`, p.ID, reason, toNullUUID(in.EventID))
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return ErrPayoutIllegalTransition
			}
			payload := events.PayoutFailedEvent{
				PayoutID: p.ID.String(), StreamerID: p.StreamerID.String(),
				AmountIDR: p.AmountIDR, FailureReason: reason,
				ProviderError: in.ProviderError, FailedAt: failedAt,
			}
			if err := enqueuePayoutEvent(ctx, tx, events.PayoutFailed, p.ID, p.StreamerID, payload); err != nil {
				return err
			}
			if err := deriveBatch(ctx, tx, p.BatchID); err != nil {
				return err
			}
			out.Status = PayoutFailed
			out.Changed = true
			return nil
		}
		return ErrPayoutIllegalTransition
	})
	if err != nil {
		return FailureOutcome{}, err
	}
	return out, nil
}

// surfaceSafeReason folds the provider failure into the frozen VARCHAR(80)
// failure_reason without ever leaking provider internals to the dashboard.
func surfaceSafeReason(reason, providerErr string) string {
	if reason == "" {
		reason = "PROVIDER_FAILURE"
	}
	if len(reason) > 80 {
		reason = reason[:80]
	}
	return reason
}

// RetryPayout lifts a NEEDS_REVIEW payout back into PROCESSING with a fresh
// lease (admin-internal path; D-057 money stays reserved throughout).
func RetryPayout(ctx context.Context, db *sql.DB, payoutID uuid.UUID, lease time.Duration) (attempts int, leaseExpiresAt time.Time, err error) {
	if lease <= 0 {
		lease = DefaultPayoutLease
	}
	err = runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, payoutID); err != nil {
			return err
		}
		p, err := loadPayout(ctx, tx, payoutID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutNotFound
		}
		if err != nil {
			return err
		}
		if p.Status != PayoutNeedsReview {
			return ErrPayoutIllegalTransition
		}
		var gwExists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM gateway_withdrawals WHERE saruman_payout_id=$1)`, payoutID).Scan(&gwExists); err != nil {
			return err
		}
		if !gwExists {
			return ErrPayoutGatewayPending
		}
		expires := time.Now().UTC().Add(lease)
		if err := tx.QueryRowContext(ctx, `UPDATE payouts SET status='PROCESSING',attempts=attempts+1,lease_expires_at=$2,failure_reason=NULL WHERE id=$1 RETURNING attempts,lease_expires_at`, payoutID, expires).Scan(&attempts, &leaseExpiresAt); err != nil {
			return err
		}
		return nil
	})
	return attempts, leaseExpiresAt, err
}

// ForceFailPayout fails a NEEDS_REVIEW payout and releases the reserve (s5)
// with no provider event to correlate (admin-internal path).
func ForceFailPayout(ctx context.Context, db *sql.DB, payoutID uuid.UUID, note string) (FailureOutcome, error) {
	out := FailureOutcome{PayoutID: payoutID}
	err := runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, payoutID); err != nil {
			return err
		}
		p, err := loadPayout(ctx, tx, payoutID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutNotFound
		}
		if err != nil {
			return err
		}
		if p.Status != PayoutNeedsReview {
			return ErrPayoutIllegalTransition
		}
		reason := "FORCE_FAILED"
		if note != "" {
			reason = "FORCE_FAILED: " + note
			if len(reason) > 80 {
				reason = reason[:80]
			}
		}
		fake := WithdrawalFailedInput{PayoutID: p.ID, StreamerID: p.StreamerID, FailureReason: reason, FailedAt: time.Now().UTC()}
		if err := postRelease(ctx, tx, p.ID, p.StreamerID, p.AmountIDR, fake); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='FAILED',failure_reason=$2 WHERE id=$1`, p.ID, reason); err != nil {
			return err
		}
		payload := events.PayoutFailedEvent{
			PayoutID: p.ID.String(), StreamerID: p.StreamerID.String(),
			AmountIDR: p.AmountIDR, FailureReason: reason, FailedAt: fake.FailedAt,
		}
		if err := enqueuePayoutEvent(ctx, tx, events.PayoutFailed, p.ID, p.StreamerID, payload); err != nil {
			return err
		}
		if err := deriveBatch(ctx, tx, p.BatchID); err != nil {
			return err
		}
		out.Status = PayoutFailed
		out.Changed = true
		return nil
	})
	if err != nil {
		return FailureOutcome{}, err
	}
	return out, nil
}

// ResolvePayout settles or fails a NEEDS_REVIEW payout (operator decision
// after a hold release). Settling runs the s4 completion; failing runs the
// s5 release. Fund-hold rows themselves are released by the tokens surface —
// this only moves the payout.
func ResolvePayout(ctx context.Context, db *sql.DB, payoutID uuid.UUID, resolveTo string) (CompletionOutcome, error) {
	resolveTo = strings.ToUpper(strings.TrimSpace(resolveTo))
	if resolveTo != PayoutSettled && resolveTo != PayoutFailed {
		return CompletionOutcome{}, ErrPayoutValidation
	}
	if resolveTo == PayoutFailed {
		fo, err := ForceFailPayout(ctx, db, payoutID, "")
		if err != nil {
			return CompletionOutcome{}, err
		}
		return CompletionOutcome{PayoutID: fo.PayoutID, Status: fo.Status, Changed: fo.Changed}, nil
	}
	out := CompletionOutcome{PayoutID: payoutID}
	err := runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, payoutID); err != nil {
			return err
		}
		p, err := loadPayout(ctx, tx, payoutID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutNotFound
		}
		if err != nil {
			return err
		}
		if p.Status != PayoutNeedsReview {
			return ErrPayoutIllegalTransition
		}
		in := WithdrawalCompletedInput{
			PayoutID: p.ID, StreamerID: p.StreamerID,
			ProviderPayoutID: p.ProviderPayoutID.String,
			BankAccountRef:   p.BankAccountID.String(),
			SettledAt:        time.Now().UTC(),
		}
		if err := ensurePayoutAccounts(ctx, tx, p.StreamerID); err != nil {
			return err
		}
		if err := postComplete(ctx, tx, p, in); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='SETTLED',settled_at=$2,provider_payout_id=$3 WHERE id=$1`, p.ID, in.SettledAt, in.ProviderPayoutID); err != nil {
			return err
		}
		payload := events.PayoutSettledEvent{
			PayoutID: p.ID.String(), StreamerID: p.StreamerID.String(),
			AmountIDR: p.AmountIDR, ProviderPayoutID: in.ProviderPayoutID,
			BankReference: in.BankAccountRef, SettledAt: in.SettledAt,
		}
		if err := enqueuePayoutEvent(ctx, tx, events.PayoutSettled, p.ID, p.StreamerID, payload); err != nil {
			return err
		}
		if err := deriveBatch(ctx, tx, p.BatchID); err != nil {
			return err
		}
		out.Status = PayoutSettled
		out.SettledAt = in.SettledAt
		out.Changed = true
		return nil
	})
	if err != nil {
		return CompletionOutcome{}, err
	}
	return out, nil
}

// RefreshProviderState re-reads the gateway mirror for a payout and applies
// the provider truth: COMPLETED settles, FAILED fails, anything else is a
// no-op refresh (lease stays as-is).
func RefreshProviderState(ctx context.Context, db *sql.DB, payoutID uuid.UUID) (CompletionOutcome, error) {
	var status, providerErr, providerPayoutID, bankRef string
	err := db.QueryRowContext(ctx, `SELECT COALESCE(gw.last_known_provider_status,''),COALESCE(gw.provider_payout_id,''),COALESCE(gw.bank_account_ref,''),COALESCE(p.status::text,'') FROM gateway_withdrawals gw JOIN payouts p ON p.id=gw.saruman_payout_id WHERE gw.saruman_payout_id=$1 ORDER BY gw.created_at DESC LIMIT 1`, payoutID).
		Scan(&status, &providerPayoutID, &bankRef, &providerErr)
	if errors.Is(err, sql.ErrNoRows) {
		return CompletionOutcome{}, ErrPayoutGatewayPending
	}
	if err != nil {
		return CompletionOutcome{}, err
	}
	switch strings.ToUpper(status) {
	case "COMPLETED":
		var streamerID uuid.UUID
		if err := db.QueryRowContext(ctx, `SELECT streamer_id FROM payouts WHERE id=$1`, payoutID).Scan(&streamerID); err != nil {
			return CompletionOutcome{}, err
		}
		return WithdrawalCompleted(ctx, db, WithdrawalCompletedInput{
			PayoutID: payoutID, StreamerID: streamerID,
			ProviderPayoutID: providerPayoutID, BankAccountRef: bankRef,
			SettledAt: time.Now().UTC(),
		})
	case "FAILED":
		var streamerID uuid.UUID
		if err := db.QueryRowContext(ctx, `SELECT streamer_id FROM payouts WHERE id=$1`, payoutID).Scan(&streamerID); err != nil {
			return CompletionOutcome{}, err
		}
		fo, err := WithdrawalFailed(ctx, db, WithdrawalFailedInput{
			PayoutID: payoutID, StreamerID: streamerID,
			FailureReason: "PROVIDER_STATUS_FAILED", ProviderError: providerErr,
			FailedAt: time.Now().UTC(),
		})
		if err != nil {
			return CompletionOutcome{}, err
		}
		return CompletionOutcome{PayoutID: fo.PayoutID, Status: fo.Status, Changed: fo.Changed}, nil
	}
	// PROCESSING / REQUESTED / unknown: nothing to apply.
	return CompletionOutcome{PayoutID: payoutID}, nil
}