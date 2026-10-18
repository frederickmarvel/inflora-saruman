package service

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/google/uuid"
)

// ReviewReconciler guards the at-least-once takedown guarantee for
// donation.reviewed.v1 (CONTRACT_DRIFT D-038). The decision route stores the
// event in the transactional outbox, but durability can still break in two
// ways after it returns: the outbox dispatcher can dead-letter the event after
// MaxAttempts failed publishes, or (defensively) a rejected row can survive
// with no outbox event committed at all. Both would silently re-air removed
// content on the next overlay replay, so the reconciler re-establishes the
// enqueue deterministically — with the SAME event id the route used — and
// never invents state.
type ReviewReconciler struct {
	DB       *sql.DB
	Interval time.Duration
	Quiet    time.Duration
}

func (r *ReviewReconciler) Name() string { return "donation-review-reconciler" }

func (r *ReviewReconciler) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		r.Interval = time.Minute
	}
	if r.Quiet <= 0 {
		r.Quiet = 5 * time.Minute
	}
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resurrected, healed, err := ReconcileReviewEvents(ctx, r.DB, r.Quiet)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("review reconciler: %v", err)
				continue
			}
			if resurrected > 0 || healed > 0 {
				log.Printf("review reconciler: resurrected %d dead-lettered takedown events, healed %d rejected rows", resurrected, healed)
			}
		}
	}
}

// ReconcileReviewEvents returns how many dead-lettered takedown events were
// resurrected and how many rejected-donation rows were healed (missing enqueue
// created or its publish mark stamped).
func ReconcileReviewEvents(ctx context.Context, db *sql.DB, quiet time.Duration) (resurrected, healed int, err error) {
	if db == nil {
		return 0, 0, errors.New("review reconciler: nil database")
	}
	// Phase 1 — one-time dead-letter resurrection. A quiet window keeps the
	// reconciler from fighting a JetStream outage: only rows dead-lettered
	// longer than `quiet` ago are re-armed. The claimed_by guard keeps the
	// re-arm from stripping an in-flight dispatcher claim (the dispatcher's
	// claimOne does not exclude dead-lettered rows, so a re-arm racing that
	// claim would double-publish). The originals are resurrected in place
	// (same event id, same envelope), so there is no duplication.
	res, err := db.ExecContext(ctx, `
		UPDATE outbox_events o
		SET dead_lettered_at=NULL, published_at=NULL, claimed_at=NULL, claimed_by=NULL, attempts=0, last_error=NULL
		FROM donations d
		WHERE o.aggregate_id=d.id AND o.event_type=$1 AND d.review_status='REJECTED'
		  AND o.dead_lettered_at IS NOT NULL AND o.dead_lettered_at < NOW() - ($2 * interval '1 second')
		  AND o.claimed_by IS NULL
	`, events.DonationReviewed, int64(quiet.Seconds()))
	if err != nil {
		return 0, 0, err
	}
	if n, e := res.RowsAffected(); e == nil {
		resurrected = int(n)
	}
	// Phase 2 — heal rejected rows whose publish mark is missing.
	rows, err := db.QueryContext(ctx, `
		SELECT id FROM donations
		WHERE review_status='REJECTED' AND review_event_published_at IS NULL
		ORDER BY review_decided_at LIMIT 100
	`)
	if err != nil {
		return resurrected, 0, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return resurrected, 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return resurrected, 0, err
	}
	for _, id := range ids {
		if err := healReviewEvent(ctx, db, id); err != nil {
			return resurrected, healed, err
		}
		healed++
	}
	return resurrected, healed, nil
}

// healReviewEvent re-establishes the takedown enqueue for one rejected row and
// stamps review_event_published_at. A row whose event already exists only gets
// the mark (the dispatcher has it in flight); a row with none gets a fresh
// deterministic enqueue inside its own transaction.
func healReviewEvent(ctx context.Context, db *sql.DB, donationID string) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_events WHERE aggregate_id=$1 AND event_type=$2)`, donationID, events.DonationReviewed).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		id, err := uuid.Parse(donationID)
		if err != nil {
			return err
		}
		d, err := loadReviewDonation(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			// Deleted or never existed; nothing to heal.
			return nil
		}
		if err != nil {
			return err
		}
		if d.ReviewStatus != ReviewReject || !d.DecidedAt.Valid {
			// State moved on since the scan; nothing to do.
			return nil
		}
		if err := enqueueReviewed(ctx, tx, d, d.DecidedAt.Time); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE donations SET review_event_published_at=$2 WHERE id=$1 AND review_event_published_at IS NULL`, donationID, d.DecidedAt.Time); err != nil {
			return err
		}
		return tx.Commit()
	}
	_, err := db.ExecContext(ctx, `UPDATE donations SET review_event_published_at=NOW() WHERE id=$1 AND review_event_published_at IS NULL`, donationID)
	return err
}