package subscriber

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/google/uuid"
)

func TestProcessorRoutesFrozenTopics(t *testing.T) {
	p := &Processor{}
	for _, topic := range []string{events.DonationIntentCreated, TopupCompletedType, TopupFailedType, TopupExpiredType} {
		if _, _, err := p.route(topic); err != nil {
			t.Fatalf("route %s: %v", topic, err)
		}
	}
	if _, _, err := p.route("unknown.v1"); err == nil {
		t.Fatal("unknown topic accepted")
	}
}

func TestPostCaptureUsesCallerTransactionAndBalancesPostings(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	streamerID, donationID := uuid.NewString(), uuid.NewString()
	eventID := uuid.NewString()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_transactions")).WillReturnResult(driver.RowsAffected(1))
	for _, account := range []string{"DONOR_CASH", "STREAMER_PENDING", "PLATFORM_REVENUE", "MDR_REVENUE"} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM ledger_accounts")).WithArgs(streamerID, account).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uuid.New()))
	}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_entries")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE ledger_accounts")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_entries")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE ledger_accounts")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_entries")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE ledger_accounts")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_entries")).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE ledger_accounts")).WillReturnResult(driver.RowsAffected(1))
	ids, _, err := postCapture(context.Background(), tx, events.Envelope{EventID: eventID}, topupCompleted{DonationID: donationID, StreamerID: streamerID, GrossChargedIDR: 10070, NetIDR: 9999, PlatformFeeIDR: 1, MDRIDR: 70})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Fatalf("entry count = %d, want 4", len(ids))
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnvelopePayloadIsObject(t *testing.T) {
	payload, _ := json.Marshal(events.DonationFailedEvent{DonationID: uuid.NewString()})
	if len(payload) == 0 || payload[0] != '{' {
		t.Fatal("payload is not object")
	}
}

func TestValidateEnvelope(t *testing.T) {
	streamerID := uuid.NewString()
	valid := events.Envelope{EventID: uuid.NewString(), EventType: TopupCompletedType, EventVersion: "1", Producer: "palantir-gateway", StreamerID: &streamerID, Payload: json.RawMessage(`{}`)}
	if err := validateEnvelope(valid); err != nil {
		t.Fatal(err)
	}
	valid.EventVersion = "2"
	if err := validateEnvelope(valid); err == nil {
		t.Fatal("unsupported version accepted")
	}
}

// envelopeArg matches an outbox envelope argument when its JSON contains every
// needle and none of the excludes — used to pin the donation.charged.v1 wire
// payload on the post-hoc review fields without decoding inside the mock.
type envelopeArg struct {
	needles  []string
	excludes []string
}

func (m envelopeArg) Match(v driver.Value) bool {
	var s string
	switch b := v.(type) {
	case []byte:
		s = string(b)
	case string:
		s = b
	default:
		return false
	}
	for _, n := range m.needles {
		if !strings.Contains(s, n) {
			return false
		}
	}
	for _, n := range m.excludes {
		if strings.Contains(s, n) {
			return false
		}
	}
	return true
}

// TestCompleteTopupRecordsPostHocReview verifies the charge path stamps every
// donation APPROVED (aired immediately, CONTRACT_DRIFT D-038), records the
// moderation flag on linked donations without ever gating the transition, and
// carries both fields on the donation.charged.v1 payload.
func TestCompleteTopupRecordsPostHocReview(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	streamerID, donationID, intentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	capturedAt := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	p := &Processor{DB: db}

	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// loadDonation: 24-column row, INTENT_CREATED, linked message.
	donationCols := []string{"id", "intent_id", "streamer_id", "status", "currency", "amount_idr", "mdr_idr", "mdr_rate_bps", "gross_charged_idr", "platform_fee_idr", "net_idr", "donor_display_name", "is_anonymous", "message", "display_rate_idr_per_sec", "display_min_sec", "display_max_sec", "display_duration_sec", "voice_url", "voice_duration_sec", "youtube_url", "youtube_start_sec", "youtube_end_sec", "payment_method"}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id,intent_id,streamer_id,status,currency,amount_idr")).
		WithArgs(donationID).
		WillReturnRows(sqlmock.NewRows(donationCols).AddRow(donationID, intentID, streamerID, "INTENT_CREATED", "IDR", 50000, 0, 0, 0, 0, 0, "Andi", false, "visit www.scam.gg", 1000, 5, 60, 25, "", nil, "", nil, nil, "QRIS"))

	// ensureAccounts
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_accounts")).WillReturnResult(driver.RowsAffected(6))

	// The charge UPDATE now passes 11 params: the flag reason rides along as
	// NULLIF($11,'') while review_status/review_aired_at are SQL literals.
	mock.ExpectExec(regexp.QuoteMeta("UPDATE donations SET status='CHARGED'")).
		WithArgs(donationID, int64(70), int64(70), int64(50070), int64(5000), int64(45000), "midtrans", "charge-id", "QRIS", capturedAt, "Contains a link").
		WillReturnResult(driver.RowsAffected(1))

	// postCapture
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_transactions")).WillReturnResult(driver.RowsAffected(1))
	for _, account := range []string{"DONOR_CASH", "STREAMER_PENDING", "PLATFORM_REVENUE", "MDR_REVENUE"} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM ledger_accounts")).WithArgs(streamerID, account).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uuid.New()))
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO ledger_entries")).WillReturnResult(driver.RowsAffected(1))
		mock.ExpectExec(regexp.QuoteMeta("UPDATE ledger_accounts")).WillReturnResult(driver.RowsAffected(1))
	}

	// outbox enqueue: pin the charged envelope on the review fields.
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO outbox_events")).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			envelopeArg{needles: []string{`"event_type":"donation.charged.v1"`, `"review_status":"APPROVED"`, `"review_flag_reason":"Contains a link"`}},
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))

	payload := topupCompleted{DonationID: donationID, IntentID: intentID, StreamerID: streamerID, AmountIDR: 50000, NetIDR: 45000, PlatformFeeIDR: 5000, MDRIDR: 70, MDRRateBPS: 70, GrossChargedIDR: 50070, Currency: "IDR", Provider: "midtrans", ProviderChargeID: "charge-id", PaymentMethod: "QRIS", CapturedAt: capturedAt}
	payloadRaw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	env := events.Envelope{EventID: uuid.NewString(), EventType: TopupCompletedType, EventVersion: "1", Producer: "palantir-gateway", StreamerID: &streamerID, Payload: payloadRaw}
	if err := p.completeTopup(context.Background(), tx, env); err != nil {
		t.Fatal(err)
	}
	mock.ExpectCommit()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
