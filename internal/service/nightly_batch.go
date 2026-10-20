package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
)

// RunNightlyBatch implements the 00:00-UTC withdrawal sweep (CONTRACT_DRIFT
// D-062): per UTC date ONE CreatePayout per eligible streamer for the TOTAL
// available balance, all gathered into ONE DRAFT batch marked
// {"batch_kind":"nightly","run_date":runDate}; the admin executes the batch
// manually (D-063). Every guard is DB-side so a concurrent/re-launched
// worker is safe by construction:
//   - the marker row (+ the 00017 partial unique index) makes a second run
//     for the same date a no-op;
//   - the deterministic payout id (keys.PayoutRequestIDKey with key
//     "nightly-sweep:<yyyy-mm-dd>") is checked BEFORE the amount is read
//     again — CreatePayout never recomputes an amount for an existing id.
//
// Eligible: active streamer, first verified bank account (is_primary DESC,
// created_at ASC), available >= MinPayoutIDR, no ACTIVE STREAMER/PAYOUT
// fund hold. Per-candidate eligibility failures inside CreatePayout are
// benign (log-and-skip); any other error aborts the run and the next tick
// resumes it (marker absent, already-created payout ids re-skipped).
func RunNightlyBatch(ctx context.Context, db *sql.DB, now time.Time, actorAdminID uuid.UUID) (created int, batchID uuid.UUID, skipped []string, err error) {
	if actorAdminID == uuid.Nil {
		return 0, uuid.Nil, nil, ErrPayoutValidation
	}
	runDate := now.UTC().Format("2006-01-02")
	var marker uuid.UUID
	err = db.QueryRowContext(ctx, `SELECT id FROM payout_batches WHERE metadata->>'batch_kind'='nightly' AND metadata->>'run_date'=$1 LIMIT 1`, runDate).Scan(&marker)
	switch {
	case err == nil:
		return 0, marker, nil, nil
	case !errors.Is(err, sql.ErrNoRows):
		return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: marker check: %w", err)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT s.id, la.balance_idr, bank.id
		FROM streamers s
		JOIN ledger_accounts la ON la.streamer_id = s.id
		  AND la.account_type = 'STREAMER_AVAILABLE' AND la.currency = 'IDR'
		JOIN LATERAL (
			SELECT b.id FROM streamer_bank_accounts b
			WHERE b.streamer_id = s.id AND b.is_verified
			ORDER BY b.is_primary DESC, b.created_at ASC
			LIMIT 1
		) bank ON TRUE
		WHERE s.is_active = TRUE
		  AND la.balance_idr >= $1
		  AND NOT EXISTS (
			SELECT 1 FROM fund_holds fh
			WHERE fh.streamer_id = s.id AND fh.status = 'ACTIVE'
			  AND fh.target_type IN ('STREAMER','PAYOUT')
		  )
		ORDER BY s.id
		FOR UPDATE OF s SKIP LOCKED
	`, MinPayoutIDR)
	if err != nil {
		return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: candidates: %w", err)
	}
	type candidate struct {
		streamerID uuid.UUID
		available  int64
		bankID     uuid.UUID
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.streamerID, &c.available, &c.bankID); err != nil {
			rows.Close()
			return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: scan candidates: %w", err)
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: candidates: %w", err)
	}

	idemKey := "nightly-sweep:" + runDate
	var payoutIDs []uuid.UUID
	for _, c := range candidates {
		// Deterministic id checked BEFORE any amount use: a prior run of this
		// date already swept this streamer, so never touch its amount.
		payoutIDStr := keys.PayoutRequestIDKey(c.streamerID.String(), idemKey)
		var existing int
		err := db.QueryRowContext(ctx, `SELECT 1 FROM payouts WHERE id=$1`, payoutIDStr).Scan(&existing)
		switch {
		case err == nil:
			skipped = append(skipped, c.streamerID.String())
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: payout existence: %w", err)
		}
		res, err := CreatePayout(ctx, db, CreatePayoutRequest{
			StreamerID:     c.streamerID,
			AmountIDR:      c.available,
			BankAccountID:  c.bankID,
			IdempotencyKey: idemKey,
		})
		if err != nil {
			if isBenignNightlySkip(err) {
				skipped = append(skipped, c.streamerID.String())
				continue
			}
			return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: create payout for %s: %w", c.streamerID, err)
		}
		payoutIDs = append(payoutIDs, res.PayoutID)
	}
	if len(payoutIDs) == 0 {
		return 0, uuid.Nil, skipped, nil
	}
	out, err := CreateBatch(ctx, db, actorAdminID, payoutIDs, map[string]string{"batch_kind": "nightly", "run_date": runDate})
	if err != nil {
		return 0, uuid.Nil, nil, fmt.Errorf("nightly-batch: create batch: %w", err)
	}
	return len(payoutIDs), out.BatchID, skipped, nil
}

// isBenignNightlySkip reports whether a CreatePayout error means "this
// streamer is simply not withdrawable right now" — skip and continue —
// rather than a database/persistence failure that should abort the run.
func isBenignNightlySkip(err error) bool {
	return errors.Is(err, ErrPayoutBelowMinimum) ||
		errors.Is(err, ErrPayoutInsufficientBalance) ||
		errors.Is(err, ErrPayoutHoldActive) ||
		errors.Is(err, ErrPayoutBankNotFound) ||
		errors.Is(err, ErrPayoutBankNotVerified) ||
		errors.Is(err, ErrPayoutIdempotencyConflict)
}

// NightlyBatcher is the saruman worker that runs the daily withdrawal sweep
// (D-062). It ticks every CheckInterval while the wall clock (UTC) is in the
// configured hour and calls RunNightlyBatch; ALL per-date idempotency lives
// in the DB (batch marker + deterministic payout ids), so re-ticks are
// no-ops and an admin deleting the nightly batch re-enables a same-date
// re-sweep by the next tick. There is deliberately no in-memory
// already-ran-today guard — one would break that delete+re-sweep contract —
// so each in-hour tick costs one cheap marker SELECT.
type NightlyBatcher struct {
	DB            *sql.DB
	HourUTC       int
	CheckInterval time.Duration
	ActorAdminID  uuid.UUID
}

func (n *NightlyBatcher) Name() string { return "nightly-batcher" }

func (n *NightlyBatcher) Run(ctx context.Context) error {
	interval := n.CheckInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := n.tick(ctx); err != nil && ctx.Err() == nil {
				log.Printf("nightly-batcher: tick failed: %v", err)
			}
		}
	}
}

func (n *NightlyBatcher) tick(ctx context.Context) error {
	now := time.Now().UTC()
	if now.Hour() != n.HourUTC {
		return nil
	}
	runDate := now.Format("2006-01-02")
	created, batchID, skipped, err := RunNightlyBatch(ctx, n.DB, now, n.ActorAdminID)
	if err != nil {
		return err
	}
	switch {
	case created > 0:
		log.Printf("nightly-batcher: %s batch %s with %d payouts (%d skipped)", runDate, batchID, created, len(skipped))
	case batchID != uuid.Nil:
		log.Printf("nightly-batcher: %s batch %s already exists", runDate, batchID)
	case len(skipped) > 0:
		log.Printf("nightly-batcher: %s nothing eligible (%d skipped)", runDate, len(skipped))
	}
	return nil
}