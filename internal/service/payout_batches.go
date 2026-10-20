package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/google/uuid"
)

// Batch statuses (payout_batch_status enum, frozen in 00001).
const (
	BatchDraft         = "DRAFT"
	BatchExecuting     = "EXECUTING"
	BatchCompleted     = "COMPLETED"
	BatchPartialFailed = "PARTIAL_FAILED"
	BatchFailed        = "FAILED"
)

type batchRow struct {
	ID            uuid.UUID
	Status        string
	TotalAmountIDR int64
	PayoutCount   int
	CreatedBy     uuid.UUID
	ProviderName  sql.NullString
	ProviderBatchID sql.NullString
	CreatedAt     time.Time
	ExecutingAt   sql.NullTime
	CompletedAt   sql.NullTime
	FailureReason sql.NullString
}

func loadBatch(ctx context.Context, tx *sql.Tx, id uuid.UUID) (batchRow, error) {
	var b batchRow
	err := tx.QueryRowContext(ctx, `SELECT id,status::text,total_amount_idr,payout_count,created_by,provider_name,provider_batch_id,created_at,executing_at,completed_at,failure_reason FROM payout_batches WHERE id=$1 FOR UPDATE`, id).
		Scan(&b.ID, &b.Status, &b.TotalAmountIDR, &b.PayoutCount, &b.CreatedBy, &b.ProviderName, &b.ProviderBatchID, &b.CreatedAt, &b.ExecutingAt, &b.CompletedAt, &b.FailureReason)
	return b, err
}

// CreateBatchResult reports the new batch and how many payouts it took.
type CreateBatchResult struct {
	BatchID        uuid.UUID
	PayoutCount    int
	TotalAmountIDR int64
}

// CreateBatch creates a finops payout batch, optionally seeding it with
// REQUESTED payouts (each claimed and moved to BATCHED with counters and a
// payout.batched.v1 event inside the same transaction). metadata (nil for
// none) is stored as JSONB — the nightly batcher marks its runs with
// {"batch_kind":"nightly","run_date":"YYYY-MM-DD"} so the partial unique
// index (00017) enforces one batch per date.
func CreateBatch(ctx context.Context, db *sql.DB, createdBy uuid.UUID, payoutIDs []uuid.UUID, metadata map[string]string) (CreateBatchResult, error) {
	if createdBy == uuid.Nil {
		return CreateBatchResult{}, ErrPayoutValidation
	}
	var metadataJSON []byte
	var err error
	if len(metadata) > 0 {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return CreateBatchResult{}, fmt.Errorf("payout: batch metadata: %w", err)
		}
	}
	out := CreateBatchResult{BatchID: uuid.New()}
	err = runSerializable(ctx, db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO payout_batches (id,created_by,metadata) VALUES ($1,$2,$3)`, out.BatchID, createdBy, metadataJSON)
		if err != nil {
			return fmt.Errorf("payout: create batch: %w", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return errors.New("payout: batch insert affected no rows")
		}
		var total int64
		for pos, pid := range payoutIDs {
			amount, err := assignToBatch(ctx, tx, out.BatchID, pid, pos)
			if err != nil {
				return err
			}
			total += amount
		}
		if _, err := tx.ExecContext(ctx, `UPDATE payout_batches SET total_amount_idr=$2,payout_count=$3 WHERE id=$1`, out.BatchID, total, len(payoutIDs)); err != nil {
			return fmt.Errorf("payout: batch counters: %w", err)
		}
		out.PayoutCount = len(payoutIDs)
		out.TotalAmountIDR = total
		return nil
	})
	return out, err
}

// assignToBatch moves one REQUESTED payout into a DRAFT batch (BATCHED) and
// emits payout.batched.v1. It returns the member amount for the running
// counter; the caller updates replicate counters after the loop.
func assignToBatch(ctx context.Context, tx *sql.Tx, batchID, payoutID uuid.UUID, fifoPosition int) (int64, error) {
	if err := claimSequentially(ctx, tx, payoutID); err != nil {
		return 0, err
	}
	p, err := loadPayout(ctx, tx, payoutID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrPayoutNotFound
	}
	if err != nil {
		return 0, err
	}
	if p.Status != PayoutRequested || p.BatchID.Valid {
		return 0, ErrPayoutIllegalTransition
	}
	res, err := tx.ExecContext(ctx, `UPDATE payouts SET status='BATCHED',batch_id=$2 WHERE id=$1 AND status='REQUESTED' AND batch_id IS NULL`, payoutID, batchID)
	if err != nil {
		return 0, fmt.Errorf("payout: assign: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return 0, ErrPayoutIllegalTransition
	}
	var bankLast4 string
	if err := tx.QueryRowContext(ctx, `SELECT account_number_last4 FROM streamer_bank_accounts WHERE id=$1`, p.BankAccountID).Scan(&bankLast4); err != nil {
		return 0, fmt.Errorf("payout: batch bank lookup: %w", err)
	}
	// The running batch total INCLUDING this member — prior members assigned in
	// the same transaction are visible here, so the event's BatchTotalIDR is
	// always the batch's true total at assignment time.
	var batchTotal int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(amount_idr),0) FROM payouts WHERE batch_id=$1`, batchID).Scan(&batchTotal); err != nil {
		return 0, fmt.Errorf("payout: batch total lookup: %w", err)
	}
	payload := events.PayoutBatchedEvent{
		BatchID: batchID.String(), PayoutID: payoutID.String(), StreamerID: p.StreamerID.String(),
		AmountIDR: p.AmountIDR, BankAccountLast4: bankLast4, BatchTotalIDR: batchTotal,
		BatchPayoutCount: fifoPosition + 1, FIFOPosition: fifoPosition, BatchedAt: time.Now().UTC(),
	}
	if err := enqueuePayoutEvent(ctx, tx, events.PayoutBatched, payoutID, p.StreamerID, payload); err != nil {
		return 0, err
	}
	return p.AmountIDR, nil
}

// AddToBatch appends REQUESTED payouts to a DRAFT batch. Returns the new
// running total and count after the assignment.
func AddToBatch(ctx context.Context, db *sql.DB, batchID uuid.UUID, payoutIDs []uuid.UUID) (total int64, count int, err error) {
	err = runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, batchID); err != nil {
			return err
		}
		b, err := loadBatch(ctx, tx, batchID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutBatchNotFound
		}
		if err != nil {
			return err
		}
		if b.Status != BatchDraft {
			return ErrPayoutBatchIllegalState
		}
		total = b.TotalAmountIDR
		count = b.PayoutCount
		for _, pid := range payoutIDs {
			amount, err := assignToBatch(ctx, tx, batchID, pid, count)
			if err != nil {
				return err
			}
			total += amount
			count++
		}
		_, err = tx.ExecContext(ctx, `UPDATE payout_batches SET total_amount_idr=$2,payout_count=$3 WHERE id=$1`, batchID, total, count)
		return err
	})
	return total, count, err
}

// RemoveFromBatch un-assigns one member of a DRAFT batch back to REQUESTED.
func RemoveFromBatch(ctx context.Context, db *sql.DB, batchID, payoutID uuid.UUID) error {
	return runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, batchID); err != nil {
			return err
		}
		if err := claimSequentially(ctx, tx, payoutID); err != nil {
			return err
		}
		b, err := loadBatch(ctx, tx, batchID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutBatchNotFound
		}
		if err != nil {
			return err
		}
		if b.Status != BatchDraft {
			return ErrPayoutBatchIllegalState
		}
		var amount int64
		err = tx.QueryRowContext(ctx, `UPDATE payouts SET status='REQUESTED',batch_id=NULL WHERE id=$1 AND batch_id=$2 AND status='BATCHED' RETURNING amount_idr`, payoutID, batchID).Scan(&amount)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutIllegalTransition
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE payout_batches SET total_amount_idr=total_amount_idr-$2,payout_count=payout_count-1 WHERE id=$1`, batchID, amount)
		return err
	})
}

// CancelBatch reverts a DRAFT batch's members to REQUESTED and deletes the
// row (payout_batch_status has no cancelled state; 03 §2.2: no batch
// deletion after DRAFT→EXECUTING). On an EXECUTING batch, members still in
// BATCHED revert to REQUESTED with counters adjusted and the row kept —
// PROCESSING members own live money movement and stay untouched (they exit
// via their own events or admin resolution).
func CancelBatch(ctx context.Context, db *sql.DB, batchID uuid.UUID) error {
	return runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, batchID); err != nil {
			return err
		}
		b, err := loadBatch(ctx, tx, batchID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutBatchNotFound
		}
		if err != nil {
			return err
		}
		switch b.Status {
		case BatchDraft:
			if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='REQUESTED',batch_id=NULL WHERE batch_id=$1 AND status='BATCHED'`, batchID); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM payout_batches WHERE id=$1 AND status='DRAFT'`, batchID)
			return err
		case BatchExecuting:
			var reverted, amount int64
			if err := tx.QueryRowContext(ctx, `WITH undone AS (UPDATE payouts SET status='REQUESTED',batch_id=NULL WHERE batch_id=$1 AND status='BATCHED' RETURNING amount_idr) SELECT COUNT(*),COALESCE(SUM(amount_idr),0) FROM undone`, batchID).Scan(&reverted, &amount); err != nil {
				return err
			}
			if reverted > 0 {
				if _, err := tx.ExecContext(ctx, `UPDATE payout_batches SET total_amount_idr=total_amount_idr-$2,payout_count=payout_count-$3 WHERE id=$1`, batchID, amount, reverted); err != nil {
					return err
				}
			}
			return nil
		default:
			return ErrPayoutBatchIllegalState
		}
	})
}

// SubmitBatch executes a DRAFT batch: every member moves BATCHED->PROCESSING
// with attempt + lease raise and the gateway-row assert, and emits
// payout.batch.executed.v1 per member (D-055 — provider work happened at
// request time; this is the submit bookkeeping).
func SubmitBatch(ctx context.Context, db *sql.DB, batchID uuid.UUID, lease time.Duration) error {
	if lease <= 0 {
		lease = DefaultPayoutLease
	}
	return runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, batchID); err != nil {
			return err
		}
		b, err := loadBatch(ctx, tx, batchID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutBatchNotFound
		}
		if err != nil {
			return err
		}
		if b.Status != BatchDraft {
			return ErrPayoutBatchIllegalState
		}
		if b.PayoutCount <= 0 {
			return fmt.Errorf("%w: empty batch cannot execute", ErrPayoutBatchIllegalState)
		}
		rows, err := tx.QueryContext(ctx, `SELECT p.id,p.streamer_id,p.amount_idr,COALESCE(gw.gateway_exists,FALSE) FROM payouts p LEFT JOIN (SELECT saruman_payout_id, TRUE AS gateway_exists FROM gateway_withdrawals) gw ON gw.saruman_payout_id=p.id WHERE p.batch_id=$1 AND p.status='BATCHED' FOR UPDATE OF p`, batchID)
		if err != nil {
			return err
		}
		expires := time.Now().UTC().Add(lease)
		executedAt := time.Now().UTC()
		var members []struct {
			id         uuid.UUID
			streamerID uuid.UUID
			amount     int64
			ok         bool
		}
		for rows.Next() {
			var m struct {
				id         uuid.UUID
				streamerID uuid.UUID
				amount     int64
				ok         bool
			}
			if err := rows.Scan(&m.id, &m.streamerID, &m.amount, &m.ok); err != nil {
				rows.Close()
				return err
			}
			members = append(members, m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(members) != b.PayoutCount {
			return ErrPayoutIllegalTransition
		}
		for _, m := range members {
			if !m.ok {
				return ErrPayoutGatewayPending
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE payout_batches SET status='EXECUTING',executing_at=NOW() WHERE id=$1 AND status='DRAFT'`, batchID); err != nil {
			return err
		}
		for _, m := range members {
			if _, err := tx.ExecContext(ctx, `UPDATE payouts SET status='PROCESSING',attempts=attempts+1,processed_at=COALESCE(processed_at,NOW()),lease_expires_at=$2 WHERE id=$1 AND status='BATCHED'`, m.id, expires); err != nil {
				return err
			}
			payload := events.PayoutBatchExecutedEvent{
				BatchID: batchID.String(), PayoutID: m.id.String(), StreamerID: m.streamerID.String(),
				AmountIDR: m.amount,
				// No provider bulk-disbursement API exists (03 §6): the fields
				// stay empty rather than fabricating a provider batch id.
				ProviderBatchID: "", ProviderName: "",
				ExecutedAt: executedAt,
			}
			if err := enqueuePayoutEvent(ctx, tx, events.PayoutBatchExecuted, m.id, m.streamerID, payload); err != nil {
				return err
			}
		}
		return nil
	})
}

// CloseBatch resolves a PARTIAL_FAILED batch with an operator note. The
// members are all terminal by the time the batch reaches PARTIAL_FAILED
// (deriveBatch only fires then), so closing is annotation + completion event.
func CloseBatch(ctx context.Context, db *sql.DB, batchID uuid.UUID, resolutionNote string) error {
	return runSerializable(ctx, db, func(tx *sql.Tx) error {
		if err := claimSequentially(ctx, tx, batchID); err != nil {
			return err
		}
		b, err := loadBatch(ctx, tx, batchID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPayoutBatchNotFound
		}
		if err != nil {
			return err
		}
		if b.Status != BatchPartialFailed {
			return ErrPayoutBatchIllegalState
		}
		res, err := tx.ExecContext(ctx, `UPDATE payout_batches SET resolution_note=$2,completed_at=NOW() WHERE id=$1 AND resolution_note IS NULL`, batchID, resolutionNote)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil // already closed: annotation and the completion event are once-only
		}
		var settled, failed int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FILTER (WHERE status='SETTLED'),COUNT(*) FILTER (WHERE status='FAILED') FROM payouts WHERE batch_id=$1`, batchID).Scan(&settled, &failed); err != nil {
			return err
		}
		payload := events.PayoutBatchCompletedEvent{
			BatchID: batchID.String(), TotalAmountIDR: b.TotalAmountIDR,
			SettledCount: settled, FailedCount: failed,
			ProviderBatchID: b.ProviderBatchID.String, CompletedAt: time.Now().UTC(),
		}
		return enqueueBatchEvent(ctx, tx, events.PayoutBatchCompleted, batchID, payload)
	})
}

// deriveBatch derives the batch outcome after a member settles or fails
// (called inside the member's transaction, so the batch row lock is already
// scoped by the outer serializable boundary). Members still in flight
// (REQUESTED/BATCHED/PROCESSING/NEEDS_REVIEW) keep the batch EXECUTING; an
// all-terminal membership flips it once: COMPLETED, FAILED, or PARTIAL_FAILED.
func deriveBatch(ctx context.Context, tx *sql.Tx, batchID uuid.NullUUID) error {
	if !batchID.Valid {
		return nil
	}
	if err := claimSequentially(ctx, tx, batchID.UUID); err != nil {
		return err
	}
	b, err := loadBatch(ctx, tx, batchID.UUID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // batch deleted under us (cancelled in parallel); nothing to derive
	}
	if err != nil {
		return err
	}
	if b.Status != BatchExecuting {
		return nil // already terminal, or not yet submitted
	}
	var total, settled, failed int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status='SETTLED'),COUNT(*) FILTER (WHERE status='FAILED') FROM payouts WHERE batch_id=$1`, batchID.UUID).Scan(&total, &settled, &failed); err != nil {
		return err
	}
	if total == 0 || settled+failed != total {
		return nil // members still in flight
	}
	now := time.Now().UTC()
	switch {
	case settled == total:
		res, err := tx.ExecContext(ctx, `UPDATE payout_batches SET status='COMPLETED',completed_at=$2 WHERE id=$1 AND status='EXECUTING'`, batchID.UUID, now)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil // derived concurrently; first write wins
		}
		payload := events.PayoutBatchCompletedEvent{
			BatchID: batchID.UUID.String(), TotalAmountIDR: b.TotalAmountIDR,
			SettledCount: settled, FailedCount: failed,
			ProviderBatchID: b.ProviderBatchID.String, CompletedAt: now,
		}
		return enqueueBatchEvent(ctx, tx, events.PayoutBatchCompleted, batchID.UUID, payload)
	case failed == total:
		res, err := tx.ExecContext(ctx, `UPDATE payout_batches SET status='FAILED',completed_at=$2,failure_reason='ALL_MEMBERS_FAILED' WHERE id=$1 AND status='EXECUTING'`, batchID.UUID, now)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil
		}
		payload := events.PayoutBatchFailedEvent{
			BatchID: batchID.UUID.String(), FailureReason: "ALL_MEMBERS_FAILED", FailedAt: now,
		}
		return enqueueBatchEvent(ctx, tx, events.PayoutBatchFailed, batchID.UUID, payload)
	default:
		res, err := tx.ExecContext(ctx, `UPDATE payout_batches SET status='PARTIAL_FAILED' WHERE id=$1 AND status='EXECUTING'`, batchID.UUID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil
		}
		return nil // operator resolves via CloseBatch
	}
}

// enqueueBatchEvent writes a batch-scoped event (no streamer) to the outbox.
func enqueueBatchEvent(ctx context.Context, tx *sql.Tx, eventType string, batchID uuid.UUID, payload any) error {
	env, err := events.NewEnvelope(eventType, "saruman", nil, payload)
	if err != nil {
		return err
	}
	return enqueueOutbox(ctx, tx, env, "payout_batch", batchID)
}