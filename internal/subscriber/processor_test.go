package subscriber

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"testing"

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
