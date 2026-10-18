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
	"github.com/frederickmarvel/inflora-shared/inbox"
	"github.com/frederickmarvel/inflora-shared/keys"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/google/uuid"
)

const (
	IntentCreatedSubject = "donation.intent.created.v1.>"
	TopupCompletedType   = "pg.gateway.topup.completed.v1"
	TopupFailedType      = "pg.gateway.topup.failed.v1"
	TopupExpiredType     = "pg.gateway.topup.expired.v1"
)

type Processor struct{ DB *sql.DB }

func (p *Processor) Handle(ctx context.Context, env events.Envelope) error {
	if err := validateEnvelope(env); err != nil {
		return err
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	consumer, handler, err := p.route(env.EventType)
	if err != nil {
		return err
	}
	err = inbox.Handle(ctx, p.DB, consumer, raw, handler)
	if errors.Is(err, inbox.ErrNotClaimed) {
		return nil
	}
	return err
}

func validateEnvelope(env events.Envelope) error {
	if _, err := uuid.Parse(env.EventID); err != nil {
		return fmt.Errorf("subscriber: invalid event id: %w", err)
	}
	if env.EventVersion != "1" {
		return fmt.Errorf("subscriber: unsupported event version %q", env.EventVersion)
	}
	if env.StreamerID == nil || *env.StreamerID == "" {
		return errors.New("subscriber: streamer id is required")
	}
	switch env.EventType {
	case events.DonationIntentCreated:
		if env.Producer != "ingest" && env.Producer != "ingest-api" {
			return fmt.Errorf("subscriber: unexpected producer %q", env.Producer)
		}
	case TopupCompletedType, TopupFailedType, TopupExpiredType:
		if env.Producer != "palantir" && env.Producer != "palantir-gateway" {
			return fmt.Errorf("subscriber: unexpected producer %q", env.Producer)
		}
	}
	if len(env.Payload) == 0 || env.Payload[0] != '{' || !json.Valid(env.Payload) {
		return errors.New("subscriber: payload must be a JSON object")
	}
	return nil
}

func (p *Processor) route(eventType string) (string, inbox.Handler, error) {
	switch eventType {
	case events.DonationIntentCreated:
		return "saruman.donation-intent-created", p.validateIntent, nil
	case TopupCompletedType:
		return "saruman.topup-completed", p.completeTopup, nil
	case TopupFailedType:
		return "saruman.topup-failed", p.failTopup, nil
	case TopupExpiredType:
		return "saruman.topup-expired", p.expireTopup, nil
	default:
		return "", nil, fmt.Errorf("subscriber: unsupported event type %q", eventType)
	}
}

func (p *Processor) validateIntent(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload events.DonationIntentCreatedEvent
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("decode intent: %w", err)
	}
	var id, intentID, streamerID, status, currency string
	var amount int64
	var rate sql.NullInt64
	var minSec, maxSec, duration sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT id,intent_id,streamer_id,status,currency,amount_idr,display_rate_idr_per_sec,display_min_sec,display_max_sec,display_duration_sec FROM donations WHERE id=$1 AND intent_id=$2 FOR UPDATE`, payload.DonationID, payload.IntentID).
		Scan(&id, &intentID, &streamerID, &status, &currency, &amount, &rate, &minSec, &maxSec, &duration)
	if err != nil {
		return fmt.Errorf("validate existing donation: %w", err)
	}
	validStatus := status == "INTENT_CREATED" || status == "CHARGED" || status == "FAILED" || status == "SETTLED" || status == "REFUNDED"
	if id != payload.DonationID || intentID != payload.IntentID || streamerID != payload.StreamerID || !validStatus || currency != "IDR" || amount != payload.AmountIDR ||
		(rate.Valid && rate.Int64 != payload.DisplayRateIDRPerSec) || (minSec.Valid && minSec.Int64 != int64(payload.DisplayMinSec)) ||
		(maxSec.Valid && maxSec.Int64 != int64(payload.DisplayMaxSec)) || (duration.Valid && duration.Int64 != int64(payload.DisplayDurationSec)) {
		return errors.New("intent event does not match the Ingest-created donation")
	}
	return ensureAccounts(ctx, tx, payload.StreamerID)
}

type donation struct {
	ID, IntentID, StreamerID, Status, Currency, DonorName, Message  string
	AmountIDR, MDRIDR, MDRRateBPS, GrossIDR, PlatformFeeIDR, NetIDR int64
	Anonymous                                                       bool
	DisplayRate                                                     int64
	DisplayMin, DisplayMax, DisplayDuration                         int
	VoiceURL, YouTubeURL, PaymentMethod                             string
	VoiceDuration, YouTubeStart, YouTubeEnd                         sql.NullInt64
}

func loadDonation(ctx context.Context, tx *sql.Tx, id string) (donation, error) {
	var d donation
	err := tx.QueryRowContext(ctx, `SELECT id,intent_id,streamer_id,status,currency,amount_idr,mdr_idr,mdr_rate_bps,gross_charged_idr,platform_fee_idr,net_idr,COALESCE(donor_display_name,''),is_anonymous,COALESCE(message,''),COALESCE(display_rate_idr_per_sec,0),COALESCE(display_min_sec,0),COALESCE(display_max_sec,0),COALESCE(display_duration_sec,0),COALESCE(voice_url,''),voice_duration_sec,COALESCE(youtube_url,''),youtube_start_sec,youtube_end_sec,COALESCE(payment_method::text,'') FROM donations WHERE id=$1 FOR UPDATE`, id).
		Scan(&d.ID, &d.IntentID, &d.StreamerID, &d.Status, &d.Currency, &d.AmountIDR, &d.MDRIDR, &d.MDRRateBPS, &d.GrossIDR, &d.PlatformFeeIDR, &d.NetIDR, &d.DonorName, &d.Anonymous, &d.Message, &d.DisplayRate, &d.DisplayMin, &d.DisplayMax, &d.DisplayDuration, &d.VoiceURL, &d.VoiceDuration, &d.YouTubeURL, &d.YouTubeStart, &d.YouTubeEnd, &d.PaymentMethod)
	return d, err
}

func (p *Processor) completeTopup(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload topupCompleted
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return err
	}
	d, err := loadDonation(ctx, tx, payload.DonationID)
	if err != nil {
		return fmt.Errorf("load donation: %w", err)
	}
	if d.IntentID != payload.IntentID || d.StreamerID != payload.StreamerID || d.AmountIDR != payload.AmountIDR {
		return errors.New("topup event does not match donation")
	}
	if d.Status != "INTENT_CREATED" {
		// A distinct, late provider result must never regress or overwrite an
		// already-terminal donation. Recording this inbox event as processed is
		// preferable to retrying a transition that can never become valid.
		return nil
	}
	if payload.Currency != "IDR" || payload.GrossChargedIDR != payload.NetIDR+payload.PlatformFeeIDR+payload.MDRIDR || payload.AmountIDR != payload.NetIDR+payload.PlatformFeeIDR {
		return errors.New("invalid topup amount invariant")
	}
	if env.StreamerID == nil || *env.StreamerID != payload.StreamerID || payload.AmountIDR <= 0 || payload.NetIDR <= 0 || payload.MDRIDR < 0 || payload.PlatformFeeIDR < 0 || payload.GrossChargedIDR <= 0 || payload.Provider == "" || payload.ProviderChargeID == "" || payload.PaymentMethod == "" {
		return errors.New("invalid topup payload")
	}
	if err := ensureAccounts(ctx, tx, payload.StreamerID); err != nil {
		return err
	}
	if payload.CapturedAt.IsZero() {
		payload.CapturedAt = time.Now().UTC()
	}
	// Post-hoc review state (CONTRACT_DRIFT D-038): every charged donation is
	// APPROVED at charge and airs immediately — the flag reason only badges the
	// dashboard and never delays anything. The rejection decision, if it ever
	// comes, is a replay-safe takedown handled by the review decision route.
	flagReason := service.FlagReasonForMessage(d.Message)
	if _, err = tx.ExecContext(ctx, `UPDATE donations SET status='CHARGED',settlement_status='PENDING',mdr_idr=$2,mdr_rate_bps=$3,gross_charged_idr=$4,platform_fee_idr=$5,net_idr=$6,provider_name=$7,provider_charge_id=$8,payment_method=$9,charged_at=$10,review_status='APPROVED',review_aired_at=$10,review_flag_reason=NULLIF($11,'') WHERE id=$1`, d.ID, payload.MDRIDR, payload.MDRRateBPS, payload.GrossChargedIDR, payload.PlatformFeeIDR, payload.NetIDR, payload.Provider, payload.ProviderChargeID, payload.PaymentMethod, payload.CapturedAt, flagReason); err != nil {
		return err
	}
	entryIDs, correlationID, err := postCapture(ctx, tx, env, payload)
	if err != nil {
		return err
	}
	charged := donationChargedPayload{DonationChargedEvent: events.DonationChargedEvent{DonationID: d.ID, IntentID: d.IntentID, StreamerID: d.StreamerID, AmountIDR: payload.AmountIDR, MDRIDR: payload.MDRIDR, MDRRateBPS: payload.MDRRateBPS, GrossChargedIDR: payload.GrossChargedIDR, PlatformFeeIDR: payload.PlatformFeeIDR, NetIDR: payload.NetIDR, Currency: "IDR", SettlementStatus: "PENDING", DonorDisplayName: d.DonorName, IsAnonymous: d.Anonymous, Message: d.Message, VoiceURL: d.VoiceURL, VoiceDurationSec: int(d.VoiceDuration.Int64), YouTubeURL: d.YouTubeURL, YouTubeStartSec: int(d.YouTubeStart.Int64), YouTubeEndSec: int(d.YouTubeEnd.Int64), DisplayDurationSec: d.DisplayDuration, ProviderChargeID: payload.ProviderChargeID, ProviderName: payload.Provider, PaymentMethod: payload.PaymentMethod, LedgerEntryIDs: entryIDs, LedgerCorrelationID: correlationID, CapturedAt: payload.CapturedAt, ReviewStatus: "APPROVED", ReviewFlagReason: flagReason}, DisplayRateIDRPerSec: d.DisplayRate, DisplayMinSec: d.DisplayMin, DisplayMaxSec: d.DisplayMax}
	donationID, streamerID, err := parseIDs(d.ID, d.StreamerID)
	if err != nil {
		return err
	}
	return enqueue(ctx, tx, env, events.DonationCharged, donationID, streamerID, charged)
}

type donationChargedPayload struct {
	events.DonationChargedEvent
	DisplayRateIDRPerSec int64 `json:"display_rate_idr_per_sec"`
	DisplayMinSec        int   `json:"display_min_sec"`
	DisplayMaxSec        int   `json:"display_max_sec"`
}

func ensureAccounts(ctx context.Context, tx *sql.Tx, streamerID string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO ledger_accounts (streamer_id,account_type,currency)
		SELECT $1, account_type, 'IDR' FROM unnest(ARRAY['DONOR_CASH','STREAMER_PENDING','STREAMER_AVAILABLE','STREAMER_PAID','PLATFORM_REVENUE','MDR_REVENUE']::ledger_account_type[]) account_type
		ON CONFLICT (streamer_id,account_type,currency) DO NOTHING`, streamerID)
	return err
}

func (p *Processor) failTopup(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload topupFailed
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return err
	}
	return p.markFailed(ctx, tx, env, payload.DonationID, payload.IntentID, payload.StreamerID, payload.AmountIDR, payload.Provider, payload.ProviderChargeID, payload.FailureReason, payload.FailedAt)
}

func (p *Processor) expireTopup(ctx context.Context, tx *sql.Tx, env events.Envelope) error {
	var payload topupExpired
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return err
	}
	return p.markFailed(ctx, tx, env, payload.DonationID, payload.IntentID, payload.StreamerID, payload.AmountIDR, payload.Provider, payload.ProviderChargeID, "TIMEOUT", payload.ExpiredAt)
}

func (p *Processor) markFailed(ctx context.Context, tx *sql.Tx, env events.Envelope, donationID, intentID, streamerID string, amount int64, provider, chargeID, reason string, failedAt time.Time) error {
	d, err := loadDonation(ctx, tx, donationID)
	if err != nil {
		return err
	}
	if d.IntentID != intentID || d.StreamerID != streamerID || d.AmountIDR != amount {
		return errors.New("failure event does not match donation")
	}
	if d.Status != "INTENT_CREATED" {
		return nil
	}
	if failedAt.IsZero() {
		failedAt = time.Now().UTC()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE donations SET status='FAILED',settlement_status='FAILED',provider_name=$2,provider_charge_id=NULLIF($3,''),failure_reason=$4,failed_at=$5 WHERE id=$1`, donationID, provider, chargeID, reason, failedAt); err != nil {
		return err
	}
	failed := events.DonationFailedEvent{IntentID: intentID, DonationID: donationID, StreamerID: streamerID, AmountIDR: amount, FailureReason: reason, ProviderName: provider, FailedAt: failedAt}
	donationUUID, streamerUUID, err := parseIDs(donationID, streamerID)
	if err != nil {
		return err
	}
	return enqueue(ctx, tx, env, events.DonationFailed, donationUUID, streamerUUID, failed)
}

func parseIDs(donationID, streamerID string) (uuid.UUID, uuid.UUID, error) {
	donationUUID, err := uuid.Parse(donationID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid donation id: %w", err)
	}
	streamerUUID, err := uuid.Parse(streamerID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid streamer id: %w", err)
	}
	return donationUUID, streamerUUID, nil
}

func enqueue(ctx context.Context, tx *sql.Tx, cause events.Envelope, eventType string, aggregateID, streamerID uuid.UUID, payload any) error {
	s := streamerID.String()
	env, err := events.NewEnvelope(eventType, "saruman", &s, payload)
	if err != nil {
		return err
	}
	env.CausationID = cause.EventID
	env.CorrelationID = cause.CorrelationID
	if env.CorrelationID == "" {
		env.CorrelationID = cause.EventID
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal outbox envelope: %w", err)
	}
	_, err = outbox.EnqueueRaw(ctx, tx, outbox.RawParams{
		EventID: uuid.MustParse(env.EventID), Producer: "saruman", EventType: env.EventType,
		EventVersion: env.EventVersion, AggregateType: "donation", AggregateID: aggregateID,
		StreamerID: sql.NullString{String: streamerID.String(), Valid: true},
		Subject:    events.Subject(env.EventType, streamerID.String()), Envelope: raw,
		OccurredAt: env.OccurredAt, AvailableAt: env.OccurredAt,
	})
	return err
}

func postCapture(ctx context.Context, tx *sql.Tx, env events.Envelope, p topupCompleted) ([]string, string, error) {
	opKey := keys.DonationCaptureKey(p.DonationID)
	txID := uuid.New()
	correlationID, err := uuid.Parse(env.EventID)
	if err != nil {
		return nil, "", err
	}
	metadata, err := json.Marshal(map[string]string{"source": TopupCompletedType})
	if err != nil {
		return nil, "", err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions (id,operation_key,correlation_id,causation_id,transaction_type,streamer_id,metadata,source_tag) VALUES ($1,$2,$3,$3,'DONATION_CAPTURE',$4,$5,$6) ON CONFLICT (operation_key) DO NOTHING`, txID, opKey, correlationID, p.StreamerID, metadata, ledger.SourceDonation)
	if err != nil {
		return nil, "", err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, "", errors.New("duplicate donation capture operation")
	}
	type posting struct {
		account, direction string
		amount             int64
		entryType          string
	}
	postings := []posting{{"DONOR_CASH", "DEBIT", p.GrossChargedIDR, "DONATION_DEPOSIT"}, {"STREAMER_PENDING", "CREDIT", p.NetIDR, "STREAMER_PENDING_CREDIT"}, {"PLATFORM_REVENUE", "CREDIT", p.PlatformFeeIDR, "PLATFORM_FEE"}, {"MDR_REVENUE", "CREDIT", p.MDRIDR, "MDR_FEE"}}
	var debits, credits int64
	for _, post := range postings {
		if post.direction == "DEBIT" {
			debits += post.amount
		} else {
			credits += post.amount
		}
	}
	if debits != credits {
		return nil, "", fmt.Errorf("unbalanced capture postings: debits=%d credits=%d", debits, credits)
	}
	ids := make([]string, 0, 4)
	for _, post := range postings {
		if post.amount == 0 {
			continue
		}
		var accountID uuid.UUID
		if err := tx.QueryRowContext(ctx, `SELECT id FROM ledger_accounts WHERE streamer_id=$1 AND account_type=$2 AND currency='IDR' FOR UPDATE`, p.StreamerID, post.account).Scan(&accountID); err != nil {
			return nil, "", fmt.Errorf("ledger account %s: %w", post.account, err)
		}
		entryID := uuid.New()
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries (id,streamer_id,account_id,direction,amount_idr,entry_type,external_ref,donation_id,correlation_id,ledger_transaction_id,metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'{}')`, entryID, p.StreamerID, accountID, post.direction, post.amount, post.entryType, p.DonationID, p.DonationID, correlationID, txID); err != nil {
			return nil, "", err
		}
		delta := post.amount
		if post.direction == "DEBIT" {
			delta = -delta
		}
		if _, err := tx.ExecContext(ctx, `UPDATE ledger_accounts SET balance_idr=balance_idr+$1,version=version+1,updated_at=NOW() WHERE id=$2`, delta, accountID); err != nil {
			return nil, "", err
		}
		ids = append(ids, entryID.String())
	}
	return ids, correlationID.String(), nil
}
