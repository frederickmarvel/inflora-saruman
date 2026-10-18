package service

import (
	"context"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// TestReconcileReviewEventsResurrectsAndHeals covers both phases: Phase 1
// re-arms a dead-lettered takedown event (skipping in-flight claims), Phase 2
// re-enqueues the deterministic event for a rejected row whose publish mark
// is missing.
func TestReconcileReviewEventsResurrectsAndHeals(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	donationID := uuid.NewString()
	decidedAt := time.Date(2026, 10, 18, 10, 30, 0, 0, time.UTC)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE outbox_events o")).
		WithArgs("donation.reviewed.v1", int64(300)).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM donations")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(donationID))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS(SELECT 1 FROM outbox_events")).
		WithArgs(donationID, "donation.reviewed.v1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(reviewRow(donationID, uuid.NewString(), "CHARGED", "REJECTED", decidedAt, "Contains a link"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO outbox_events")).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE donations SET review_event_published_at=$2")).
		WithArgs(donationID, decidedAt).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	resurrected, healed, err := ReconcileReviewEvents(context.Background(), db, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if resurrected != 1 || healed != 1 {
		t.Fatalf("resurrected=%d healed=%d, want 1/1", resurrected, healed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestReconcileReviewEventsStampsExistingEnqueue covers the heal lane's
// short path: the takedown event already exists in the outbox (publisher has
// it in flight or done), so only the publish mark is stamped — nothing is
// re-enqueued.
func TestReconcileReviewEventsStampsExistingEnqueue(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	donationID := uuid.NewString()

	mock.ExpectExec(regexp.QuoteMeta("UPDATE outbox_events o")).
		WithArgs("donation.reviewed.v1", int64(300)).
		WillReturnResult(driver.RowsAffected(0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM donations")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(donationID))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS(SELECT 1 FROM outbox_events")).
		WithArgs(donationID, "donation.reviewed.v1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE donations SET review_event_published_at=NOW()")).
		WithArgs(donationID).
		WillReturnResult(driver.RowsAffected(1))

	resurrected, healed, err := ReconcileReviewEvents(context.Background(), db, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if resurrected != 0 || healed != 1 {
		t.Fatalf("resurrected=%d healed=%d, want 0/1", resurrected, healed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}