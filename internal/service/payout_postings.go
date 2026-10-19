package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/frederickmarvel/inflora-saruman/internal/ledger"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// errToPG unwraps to the pgconn error when available. Serialization failures
// (40001) and unique violations (23505) surface through the driver wrapped by
// whatever the caller did — errors.As finds the concrete code regardless.
func errToPG(err error) (*pgconn.PgError, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr, true
	}
	return nil, false
}

// ensurePayoutAccounts seeds every streamer ledger account, including the
// DISBURSED account the completion posting needs (D-048). Idempotent via the
// (streamer_id, account_type, currency) unique constraint.
func ensurePayoutAccounts(ctx context.Context, tx *sql.Tx, streamerID uuid.UUID) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO ledger_accounts (streamer_id,account_type,currency)
		SELECT $1, account_type, 'IDR' FROM unnest(ARRAY['DONOR_CASH','STREAMER_PENDING','STREAMER_AVAILABLE','STREAMER_PAID','PLATFORM_REVENUE','MDR_REVENUE','DISBURSED']::ledger_account_type[]) account_type
		ON CONFLICT (streamer_id,account_type,currency) DO NOTHING`, streamerID)
	return err
}

// postReserve applies the frozen §3 payout reservation: op key
// create_payout:<id>, transaction PAYOUT_RESERVATION, DEBIT streamer_available
// and CREDIT streamer_paid, both entries STREAMER_PAID_DEBIT. The caller
// already checked the available balance under the same transaction.
func postReserve(ctx context.Context, tx *sql.Tx, payoutID, streamerID uuid.UUID, amountIDR int64, correlationID uuid.UUID, idempotencyKey string) error {
	opKey := keys.CreatePayoutKey(payoutID.String())
	meta := map[string]string{"source": "payout-command"}
	if idempotencyKey != "" {
		meta["idempotency_key"] = idempotencyKey
	}
	metadata, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	txID := uuid.New()
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_RESERVATION',$4,$5,$6) ON CONFLICT (operation_key) DO NOTHING`, txID, opKey, toNullUUID(correlationID), streamerID, metadata, ledger.SourceDonation)
	if err != nil {
		return fmt.Errorf("payout: reserve transaction: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("payout: duplicate payout reservation operation")
	}
	type posting struct{ account, direction string }
	for _, post := range []posting{{"STREAMER_AVAILABLE", "DEBIT"}, {"STREAMER_PAID", "CREDIT"}} {
		if err := postEntry(ctx, tx, postEntryParams{
			StreamerID: streamerID, AccountType: post.account, Direction: post.direction,
			AmountIDR: amountIDR, EntryType: "STREAMER_PAID_DEBIT",
			ExternalRef: payoutID.String(), PayoutID: payoutID, CorrelationID: correlationID,
			LedgerTxID: txID, Metadata: "{}",
		}); err != nil {
			return err
		}
	}
	return nil
}

// postComplete applies the frozen §4 completion as amended by D-048: op key
// complete_payout:<id>, transaction PAYOUT_COMPLETION, terminal DEBIT of
// streamer_paid (STREAMER_PAID_DEBIT) and CREDIT of the DISBURSED account
// with the new STREAMER_DISBURSED_CREDIT entry type. Corruption paranoia
// (frozen §4 tail): the paid escrow for this payout must net to zero after
// the posting.
func postComplete(ctx context.Context, tx *sql.Tx, p payoutRow, in WithdrawalCompletedInput) error {
	opKey := keys.CompletePayoutKey(p.ID.String())
	correlation := in.CorrelationID
	if correlation == uuid.Nil {
		correlation = in.EventID
	}
	meta := map[string]string{"source": events.PgGatewayWithdrawalCompleted}
	if in.ProviderPayoutID != "" {
		meta["provider_payout_id"] = in.ProviderPayoutID
	}
	metadata, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	txID := uuid.New()
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_COMPLETION',$4,$5,$6) ON CONFLICT (operation_key) DO NOTHING`, txID, opKey, toNullUUID(correlation), p.StreamerID, metadata, ledger.SourceDonation)
	if err != nil {
		return fmt.Errorf("payout: completion transaction: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("payout: duplicate payout completion operation")
	}
	// The paid escrow must hold this payout's full reserve before the
	// terminal debit (other payouts' escrow may share the account, but ours
	// cannot have leaked).
	if err := assertAccountBalanceAtLeast(ctx, tx, p.StreamerID, "STREAMER_PAID", p.AmountIDR); err != nil {
		return err
	}
	if err := postEntry(ctx, tx, postEntryParams{
		StreamerID: p.StreamerID, AccountType: "STREAMER_PAID", Direction: "DEBIT",
		AmountIDR: p.AmountIDR, EntryType: "STREAMER_PAID_DEBIT",
		ExternalRef: p.ID.String(), PayoutID: p.ID, CorrelationID: correlation,
		LedgerTxID: txID, Metadata: string(metadata),
	}); err != nil {
		return err
	}
	if err := postEntry(ctx, tx, postEntryParams{
		StreamerID: p.StreamerID, AccountType: "DISBURSED", Direction: "CREDIT",
		AmountIDR: p.AmountIDR, EntryType: "STREAMER_DISBURSED_CREDIT",
		ExternalRef: p.ID.String(), PayoutID: p.ID, CorrelationID: correlation,
		LedgerTxID: txID, Metadata: string(metadata),
	}); err != nil {
		return err
	}
	return assertPayoutEscrowZero(ctx, tx, p.ID, p.StreamerID)
}

// postRelease applies the frozen §5 failure release: op key
// fail_payout:<id>, transaction PAYOUT_FAILURE_RELEASE carrying reversal_of
// -> the §3 reservation transaction, DEBIT streamer_paid whose entry links
// reversal_of -> the §3 paid-credit entry, CREDIT streamer_available
// (STREAMER_AVAILABLE_CREDIT). The paid escrow must net to zero afterwards.
func postRelease(ctx context.Context, tx *sql.Tx, payoutID, streamerID uuid.UUID, amountIDR int64, in WithdrawalFailedInput) error {
	opKey := keys.FailPayoutKey(payoutID.String())
	var reserveTxID uuid.UUID
	err := tx.QueryRowContext(ctx, `SELECT id FROM ledger_transactions WHERE operation_key=$1`, keys.CreatePayoutKey(payoutID.String())).Scan(&reserveTxID)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("payout: reservation transaction missing for release")
	}
	if err != nil {
		return fmt.Errorf("payout: load reservation transaction: %w", err)
	}
	var reservePaidEntryID uuid.UUID
	err = tx.QueryRowContext(ctx, `SELECT e.id FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID' AND e.entry_type='STREAMER_PAID_DEBIT' ORDER BY e.created_at LIMIT 1`, payoutID).Scan(&reservePaidEntryID)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("payout: reservation paid-credit entry missing for release")
	}
	if err != nil {
		return fmt.Errorf("payout: load reservation entry: %w", err)
	}
	correlation := in.CorrelationID
	if correlation == uuid.Nil {
		correlation = in.EventID
	}
	meta := map[string]string{"source": events.PgGatewayWithdrawalFailed}
	if in.ProviderError != "" {
		meta["provider_error"] = in.ProviderError
	}
	metadata, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	txID := uuid.New()
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,reversal_of,metadata,source_tag) VALUES ($1,$2,$3,NULL,'PAYOUT_FAILURE_RELEASE',$4,$5,$6,$7) ON CONFLICT (operation_key) DO NOTHING`, txID, opKey, toNullUUID(correlation), streamerID, reserveTxID, metadata, ledger.SourceDonation)
	if err != nil {
		return fmt.Errorf("payout: release transaction: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("payout: duplicate payout release operation")
	}
	if err := postEntry(ctx, tx, postEntryParams{
		StreamerID: streamerID, AccountType: "STREAMER_PAID", Direction: "DEBIT",
		AmountIDR: amountIDR, EntryType: "STREAMER_PAID_DEBIT",
		ExternalRef: payoutID.String(), PayoutID: payoutID, CorrelationID: correlation,
		LedgerTxID: txID, Metadata: string(metadata), ReversalOf: toNullUUID(reservePaidEntryID),
	}); err != nil {
		return err
	}
	if err := postEntry(ctx, tx, postEntryParams{
		StreamerID: streamerID, AccountType: "STREAMER_AVAILABLE", Direction: "CREDIT",
		AmountIDR: amountIDR, EntryType: "STREAMER_AVAILABLE_CREDIT",
		ExternalRef: payoutID.String(), PayoutID: payoutID, CorrelationID: correlation,
		LedgerTxID: txID, Metadata: string(metadata),
	}); err != nil {
		return err
	}
	return assertPayoutEscrowZero(ctx, tx, payoutID, streamerID)
}

// postEntryParams is the ledger entry insert contract shared by the payout
// postings. Both legs of a posting reuse it; reversal legs add ReversalOf.
type postEntryParams struct {
	StreamerID    uuid.UUID
	AccountType   string
	Direction     string
	AmountIDR     int64
	EntryType     string
	ExternalRef   string
	PayoutID      uuid.UUID
	CorrelationID uuid.UUID
	LedgerTxID    uuid.UUID
	Metadata      string
	ReversalOf    any // nil => NULL
}

func postEntry(ctx context.Context, tx *sql.Tx, p postEntryParams) error {
	var accountID uuid.UUID
	if err := tx.QueryRowContext(ctx, `SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE`, p.StreamerID, p.AccountType).Scan(&accountID); err != nil {
		return fmt.Errorf("payout: ledger account %s: %w", p.AccountType, err)
	}
	entryID := uuid.New()
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,payout_id,hold_id,reversal_of,correlation_id,ledger_transaction_id,metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL,$9,$10,$11,$12)`, entryID, p.StreamerID, accountID, p.Direction, p.AmountIDR, p.EntryType, p.ExternalRef, p.PayoutID, p.ReversalOf, toNullUUID(p.CorrelationID), p.LedgerTxID, p.Metadata)
	if err != nil {
		return fmt.Errorf("payout: insert entry: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("payout: entry insert affected no rows")
	}
	delta := p.AmountIDR
	if p.Direction == "DEBIT" {
		delta = -delta
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2`, delta, accountID); err != nil {
		return fmt.Errorf("payout: update balance: %w", err)
	}
	return nil
}

// assertAccountBalanceAtLeast guards a debit leg against escrow corruption.
func assertAccountBalanceAtLeast(ctx context.Context, tx *sql.Tx, streamerID uuid.UUID, accountType string, amountIDR int64) error {
	var balance int64
	if err := tx.QueryRowContext(ctx, `SELECT balance_idr FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE`, streamerID, accountType).Scan(&balance); err != nil {
		return fmt.Errorf("payout: load %s balance: %w", accountType, err)
	}
	if balance < amountIDR {
		return fmt.Errorf("payout: %s escrow short (balance=%d need=%d)", accountType, balance, amountIDR)
	}
	return nil
}

// assertPayoutEscrowZero enforces the §4/§5 corruption check: this payout's
// net effect on streamer_paid must be zero after a terminal posting (reserve
// +x, terminal -x). Anything else means doubled or partial application.
func assertPayoutEscrowZero(ctx context.Context, tx *sql.Tx, payoutID, streamerID uuid.UUID) error {
	var net int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN e.direction='DEBIT' THEN -e.amount_idr ELSE e.amount_idr END),0) FROM ledger_entries e JOIN ledger_accounts a ON a.id=e.account_id WHERE e.payout_id=$1 AND a.account_type='STREAMER_PAID'`, payoutID).Scan(&net)
	if err != nil {
		return fmt.Errorf("payout: escrow invariant check: %w", err)
	}
	if net != 0 {
		return fmt.Errorf("payout: paid-escrow leak for payout %s (net=%d)", payoutID, net)
	}
	return nil
}

// enqueuePayoutEvent writes a payout-family event to the outbox inside the
// caller's transaction (aggregate "payout", producer "saruman").
func enqueuePayoutEvent(ctx context.Context, tx *sql.Tx, eventType string, payoutID, streamerID uuid.UUID, payload any) error {
	s := streamerID.String()
	env, err := events.NewEnvelope(eventType, "saruman", &s, payload)
	if err != nil {
		return err
	}
	return enqueueOutbox(ctx, tx, env, "payout", payoutID)
}

// enqueueOutbox builds the envelope row for outbox.EnqueueRaw, routing the
// subject per streamer scope (batch events travel with none).
func enqueueOutbox(ctx context.Context, tx *sql.Tx, env events.Envelope, aggregateType string, aggregateID uuid.UUID) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("payout: marshal outbox envelope: %w", err)
	}
	params := outbox.RawParams{
		EventID: uuid.MustParse(env.EventID), Producer: "saruman", EventType: env.EventType,
		EventVersion: env.EventVersion, AggregateType: aggregateType, AggregateID: aggregateID,
		Subject: events.Subject(env.EventType, ""), Envelope: raw,
		OccurredAt: env.OccurredAt, AvailableAt: env.OccurredAt,
	}
	if env.StreamerID != nil && *env.StreamerID != "" {
		params.Subject = events.Subject(env.EventType, *env.StreamerID)
		params.StreamerID = sql.NullString{String: *env.StreamerID, Valid: true}
	}
	if _, err := outbox.EnqueueRaw(ctx, tx, params); err != nil {
		return fmt.Errorf("payout: outbox enqueue: %w", err)
	}
	return nil
}

// toNullUUID converts uuid.Nil to SQL NULL; the ledger correlation columns
// are nullable and all-zero UUIDs would alias a real (impossible) event id.
func toNullUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}