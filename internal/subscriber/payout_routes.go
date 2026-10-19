package subscriber

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/ledger"
	"github.com/frederickmarvel/inflora-saruman/internal/service"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/google/uuid"
)

// Payout-family inbound payloads (CONTRACT_DRIFT D-055). Produced by palantir
// (or palantir-gateway); consumed exclusively by saruman. The withdrawal pair
// closes PROCESSING payout rows; pg.gateway.topup.settled.v1 is the dev-only
// payment-settlement closure (D-052) and the only settlement route in
// Phase C.

type withdrawalCompleted struct {
	PayoutID         string    `json:"payout_id"`
	StreamerID       string    `json:"streamer_id"`
	ProviderPayoutID string    `json:"provider_payout_id"`
	BankAccountRef   string    `json:"bank_account_ref"`
	SettledAt        time.Time `json:"settled_at"`
}

type withdrawalFailed struct {
	PayoutID      string    `json:"payout_id"`
	StreamerID    string    `json:"streamer_id"`
	FailureReason string    `json:"failure_reason"`
	ProviderError string    `json:"provider_error"`
	FailedAt      time.Time `json:"failed_at"`
}

type topupSettled struct {
	DonationID            string    `json:"donation_id"`
	IntentID              string    `json:"intent_id"`
	StreamerID            string    `json:"streamer_id"`
	ProviderSettlementID  string    `json:"provider_settlement_id"`
	SettledAt             time.Time `json:"settled_at"`
}

// withdrawalCompletedHandler routes pg.gateway.withdrawal.completed.v1 into
// the payout machine (PROCESSING→SETTLED with the settle-time hold gate, or
// NEEDS_REVIEW for pre-submit completions). The service call owns its own
// serializable transaction; the inbox row is the dedupe guard against
// redelivered events.
func (p *Processor) withdrawalCompletedHandler(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload withdrawalCompleted
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("decode withdrawal completed: %w", err)
	}
	payoutID, err := uuid.Parse(payload.PayoutID)
	if err != nil {
		return fmt.Errorf("invalid payout id: %w", err)
	}
	streamerID, err := uuid.Parse(payload.StreamerID)
	if err != nil {
		return fmt.Errorf("invalid streamer id: %w", err)
	}
	if env.StreamerID == nil || *env.StreamerID != payload.StreamerID {
		return errors.New("withdrawal completed event streamer mismatch")
	}
	eventID := uuid.MustParse(env.EventID)
	correlation := eventID
	if env.CorrelationID != "" {
		if parsed, err := uuid.Parse(env.CorrelationID); err == nil {
			correlation = parsed
		}
	}
	_, err = service.WithdrawalCompleted(ctx, p.DB, service.WithdrawalCompletedInput{
		PayoutID: payoutID, StreamerID: streamerID,
		ProviderPayoutID: payload.ProviderPayoutID, BankAccountRef: payload.BankAccountRef,
		SettledAt: payload.SettledAt, EventID: eventID, CorrelationID: correlation,
	})
	return err
}

// withdrawalFailedHandler routes pg.gateway.withdrawal.failed.v1 into the
// payout machine (PROCESSING→FAILED + s5 release, or NEEDS_REVIEW with the
// provider error for pre-submit failures — no release there, D-055).
func (p *Processor) withdrawalFailedHandler(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload withdrawalFailed
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("decode withdrawal failed: %w", err)
	}
	payoutID, err := uuid.Parse(payload.PayoutID)
	if err != nil {
		return fmt.Errorf("invalid payout id: %w", err)
	}
	streamerID, err := uuid.Parse(payload.StreamerID)
	if err != nil {
		return fmt.Errorf("invalid streamer id: %w", err)
	}
	if env.StreamerID == nil || *env.StreamerID != payload.StreamerID {
		return errors.New("withdrawal failed event streamer mismatch")
	}
	eventID := uuid.MustParse(env.EventID)
	correlation := eventID
	if env.CorrelationID != "" {
		if parsed, err := uuid.Parse(env.CorrelationID); err == nil {
			correlation = parsed
		}
	}
	_, err = service.WithdrawalFailed(ctx, p.DB, service.WithdrawalFailedInput{
		PayoutID: payoutID, StreamerID: streamerID,
		FailureReason: payload.FailureReason, ProviderError: payload.ProviderError,
		FailedAt: payload.FailedAt, EventID: eventID, CorrelationID: correlation,
	})
	return err
}

// settleTopupHandler applies the dev payment-closure (D-052): a CHARGED
// donation with settlement PENDING moves to SETTLED with the frozen §2
// posting (pending → available) and emits donation.settled.v1. This is the
// ONLY settlement route in Phase C.
func (p *Processor) settleTopupHandler(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload topupSettled
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("decode topup settled: %w", err)
	}
	if env.StreamerID == nil || *env.StreamerID != payload.StreamerID {
		return errors.New("topup settled event streamer mismatch")
	}
	var status, settlement, currency string
	var amount, mdr, platformFee, netIDR int64
	var intentID, donationID, providerName string
	var capturedAt time.Time
	err := tx.QueryRowContext(ctx, `SELECT id,intent_id,status::text,settlement_status::text,currency,amount_idr,mdr_idr,platform_fee_idr,net_idr,provider_name,charged_at FROM donations WHERE id=$1 FOR UPDATE`, payload.DonationID).
		Scan(&donationID, &intentID, &status, &settlement, &currency, &amount, &mdr, &platformFee, &netIDR, &providerName, &capturedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("topup settled: donation %s not found", payload.DonationID)
	}
	if err != nil {
		return err
	}
	if intentID != payload.IntentID || donationID != payload.DonationID {
		return errors.New("topup settled event does not match donation")
	}
	if status != "CHARGED" || settlement != "PENDING" {
		// Late or duplicate closure: a distinct settlement result must never
		// regress a terminal donation. Record the event and move on.
		return nil
	}
	if err := ensureAccounts(ctx, tx, payload.StreamerID); err != nil {
		return err
	}
	entryIDs, correlationID, err := postSettlement(ctx, tx, env, payload.DonationID, payload.StreamerID, netIDR)
	if err != nil {
		return err
	}
	settledAt := payload.SettledAt
	if settledAt.IsZero() {
		settledAt = time.Now().UTC()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE donations SET status='SETTLED',settlement_status='SETTLED',settled_at=$2 WHERE id=$1`, payload.DonationID, settledAt); err != nil {
		return err
	}
	settled := events.DonationSettledEvent{
		DonationID: payload.DonationID, IntentID: intentID, StreamerID: payload.StreamerID,
		AmountIDR: amount, MDRIDR: mdr, NetIDR: netIDR,
		ProviderName: providerName, ProviderSettlementID: payload.ProviderSettlementID,
		SettlementHeldSeconds: int(time.Since(capturedAt).Seconds()),
		LedgerEntryIDs: entryIDs, LedgerCorrelationID: correlationID, SettledAt: settledAt,
	}
	donationUUID, streamerUUID, err := parseIDs(payload.DonationID, payload.StreamerID)
	if err != nil {
		return err
	}
	return enqueue(ctx, tx, env, events.DonationSettled, donationUUID, streamerUUID, settled)
}

// postSettlement applies the frozen §2 settlement: DONATION_SETTLE,
// correlation = inbox event id, DEBIT streamer_pending net (reversal marker
// STREAMER_PENDING_CREDIT) + CREDIT streamer_available net
// (STREAMER_AVAILABLE_CREDIT), external_ref = donation id.
func postSettlement(ctx context.Context, tx *sql.Tx, env events.Envelope, donationID, streamerID string, netIDR int64) ([]string, string, error) {
	opKey := keys.DonationSettleKey(donationID)
	txID := uuid.New()
	correlationID, err := uuid.Parse(env.EventID)
	if err != nil {
		return nil, "", err
	}
	metadata, err := json.Marshal(map[string]string{"source": events.PgGatewayTopupSettled})
	if err != nil {
		return nil, "", err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,$3,'DONATION_SETTLE',$4,$5,$6) ON CONFLICT (operation_key) DO NOTHING`, txID, opKey, correlationID, streamerID, metadata, ledger.SourceDonation)
	if err != nil {
		return nil, "", err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, "", errors.New("duplicate donation settle operation")
	}
	entries := []struct{ account, direction, entryType string }{
		{"STREAMER_PENDING", "DEBIT", "STREAMER_PENDING_CREDIT"},
		{"STREAMER_AVAILABLE", "CREDIT", "STREAMER_AVAILABLE_CREDIT"},
	}
	ids := make([]string, 0, 2)
	for _, e := range entries {
		var accountID uuid.UUID
		if err := tx.QueryRowContext(ctx, `SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE`, streamerID, e.account).Scan(&accountID); err != nil {
			return nil, "", fmt.Errorf("settlement ledger account %s: %w", e.account, err)
		}
		entryID := uuid.New()
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,donation_id,correlation_id,ledger_transaction_id,metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'{}')`, entryID, streamerID, accountID, e.direction, netIDR, e.entryType, donationID, donationID, correlationID, txID); err != nil {
			return nil, "", err
		}
		delta := netIDR
		if e.direction == "DEBIT" {
			delta = -delta
		}
		if _, err := tx.ExecContext(ctx, `UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2`, delta, accountID); err != nil {
			return nil, "", err
		}
		ids = append(ids, entryID.String())
	}
	return ids, correlationID.String(), nil
}