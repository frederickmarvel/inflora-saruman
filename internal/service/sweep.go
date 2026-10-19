package service

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/google/uuid"
)

// SweepPayouts parks non-terminal payouts whose reservation/lease predates
// the 2x-lease threshold in NEEDS_REVIEW (D-057). The sweep never auto-fails
// anything, never releases money, and never touches rows inside the window;
// the failure reason folds the gateway mirror's last known provider status
// when it is terminal so operators see the provider truth at a glance.
func SweepPayouts(ctx context.Context, db *sql.DB, lease time.Duration) (int, error) {
	if lease <= 0 {
		lease = DefaultPayoutLease
	}
	cutoff := time.Now().UTC().Add(-2 * lease)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
		SELECT p.id, COALESCE(gw.last_known_provider_status,'')
		FROM payouts p
		LEFT JOIN (
			SELECT DISTINCT ON (saruman_payout_id) saruman_payout_id, last_known_provider_status
			FROM gateway_withdrawals ORDER BY saruman_payout_id, created_at DESC
		) gw ON gw.saruman_payout_id = p.id
		WHERE p.status IN ('REQUESTED','PROCESSING')
		  AND COALESCE(p.lease_expires_at, p.requested_at) < $1
		ORDER BY p.requested_at
		FOR UPDATE OF p SKIP LOCKED`, cutoff)
	if err != nil {
		return 0, err
	}
	type flagged struct {
		id             uuid.UUID
		providerStatus string
	}
	var targets []flagged
	for rows.Next() {
		var f flagged
		if err := rows.Scan(&f.id, &f.providerStatus); err != nil {
			rows.Close()
			return 0, err
		}
		targets = append(targets, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	flaggedCount := 0
	for _, f := range targets {
		reason := "STALE_PROVIDER"
		if f.providerStatus != "" && f.providerStatus != "PROCESSING" && f.providerStatus != "REQUESTED" {
			reason = "PROVIDER_STATUS: " + f.providerStatus
			if len(reason) > 80 {
				reason = reason[:80]
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE payouts SET status='NEEDS_REVIEW',failure_reason=$2 WHERE id=$1 AND status IN ('REQUESTED','PROCESSING')`, f.id, reason)
		if err != nil {
			return 0, err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			flaggedCount++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return flaggedCount, nil
}

// Sweeper is the periodic sweep worker (ReviewReconciler pattern).
type Sweeper struct {
	DB          *sql.DB
	Interval    time.Duration
	LeaseWindow time.Duration
}

func (s *Sweeper) Name() string { return "payout-sweeper" }

func (s *Sweeper) Run(ctx context.Context) error {
	interval := s.Interval
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
			count, err := SweepPayouts(ctx, s.DB, s.LeaseWindow)
			if err != nil && ctx.Err() == nil {
				log.Printf("payout-sweeper: sweep failed: %v", err)
				continue
			}
			if count > 0 {
				log.Printf("payout-sweeper: flagged %d stale payouts", count)
			}
		}
	}
}