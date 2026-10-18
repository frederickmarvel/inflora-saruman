package service

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

var reviewCols = []string{"id", "intent_id", "streamer_id", "status", "review_status", "amount_idr", "mdr_idr", "gross_charged_idr", "platform_fee_idr", "net_idr", "donor_display_name", "is_anonymous", "message", "voice_url", "voice_duration_sec", "youtube_url", "youtube_start_sec", "youtube_end_sec", "display_duration_sec", "settlement_status", "payment_method", "review_flag_reason", "review_aired_at", "review_decided_at"}

func reviewRow(donationID, streamerID, status, reviewStatus string, decidedAt any, flag any) *sqlmock.Rows {
	airedAt := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	return sqlmock.NewRows(reviewCols).AddRow(
		donationID, uuid.NewString(), streamerID, status, reviewStatus,
		50000, 70, 50070, 5000, 45000, "Andi", false,
		"visit www.scam.gg", "", 0, "", 0, 0, 25, "PENDING", "QRIS",
		flag, airedAt, decidedAt,
	)
}

func TestApplyDecisionRejectedEnqueuesTakedown(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	streamerID, donationID := uuid.NewString(), uuid.NewString()
	decidedAt := time.Date(2026, 10, 18, 10, 30, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(reviewRow(donationID, streamerID, "CHARGED", "APPROVED", nil, "Contains a link"))
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE donations SET review_status='REJECTED'")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"review_decided_at"}).AddRow(decidedAt))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO outbox_events")).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE donations SET review_event_published_at")).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	out, err := ApplyDecision(context.Background(), db, uuid.MustParse(donationID), uuid.MustParse(streamerID), "rejected")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Changed || out.Decision != ReviewReject || out.DonationID != donationID {
		t.Fatalf("outcome = %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyDecisionRejectIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	streamerID, donationID := uuid.NewString(), uuid.NewString()
	decidedAt := time.Date(2026, 10, 18, 10, 30, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(reviewRow(donationID, streamerID, "CHARGED", "REJECTED", decidedAt, "Contains a link"))
	mock.ExpectRollback()

	out, err := ApplyDecision(context.Background(), db, uuid.MustParse(donationID), uuid.MustParse(streamerID), "REJECTED")
	if err != nil {
		t.Fatal(err)
	}
	if out.Changed {
		t.Fatalf("expected idempotent no-change, got %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyDecisionApproveAcknowledgesWithoutEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	streamerID, donationID := uuid.NewString(), uuid.NewString()
	decidedAt := time.Date(2026, 10, 18, 10, 31, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(reviewRow(donationID, streamerID, "CHARGED", "APPROVED", nil, "Contains a link"))
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE donations SET review_decided_at=NOW()")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"review_decided_at"}).AddRow(decidedAt))
	mock.ExpectCommit()

	out, err := ApplyDecision(context.Background(), db, uuid.MustParse(donationID), uuid.MustParse(streamerID), "APPROVED")
	if err != nil {
		t.Fatal(err)
	}
	if !out.Changed || out.Decision != ReviewApprove {
		t.Fatalf("outcome = %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyDecisionGuardRails(t *testing.T) {
	t.Run("approve after reject is terminal", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		streamerID, donationID := uuid.NewString(), uuid.NewString()
		decidedAt := time.Date(2026, 10, 18, 10, 30, 0, 0, time.UTC)
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(reviewRow(donationID, streamerID, "CHARGED", "REJECTED", decidedAt, "Contains a link"))
		mock.ExpectRollback()
		_, err = ApplyDecision(context.Background(), db, uuid.MustParse(donationID), uuid.MustParse(streamerID), "APPROVED")
		if !errors.Is(err, ErrReviewTerminal) {
			t.Fatalf("err = %v, want ErrReviewTerminal", err)
		}
	})
	t.Run("never-aired donation cannot be decided", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		streamerID, donationID := uuid.NewString(), uuid.NewString()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(reviewRow(donationID, streamerID, "INTENT_CREATED", "", nil, nil))
		mock.ExpectRollback()
		_, err = ApplyDecision(context.Background(), db, uuid.MustParse(donationID), uuid.MustParse(streamerID), "REJECTED")
		if !errors.Is(err, ErrReviewNotAired) {
			t.Fatalf("err = %v, want ErrReviewNotAired", err)
		}
	})
	t.Run("other streamers donation is a not-found", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		_, donationID := uuid.NewString(), uuid.NewString()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT d.id,d.intent_id")).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(reviewRow(donationID, uuid.NewString(), "CHARGED", "APPROVED", nil, nil))
		mock.ExpectRollback()
		_, err = ApplyDecision(context.Background(), db, uuid.MustParse(donationID), uuid.MustParse(uuid.NewString()), "REJECTED")
		if !errors.Is(err, ErrReviewNotFound) {
			t.Fatalf("err = %v, want ErrReviewNotFound", err)
		}
	})
	t.Run("invalid decision literals", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		_, err = ApplyDecision(context.Background(), db, uuid.MustParse(uuid.NewString()), uuid.MustParse(uuid.NewString()), "maybe")
		if !errors.Is(err, ErrReviewBadDecision) {
			t.Fatalf("err = %v, want ErrReviewBadDecision", err)
		}
	})
}

func TestReviewedEventIDIsDeterministic(t *testing.T) {
	donationID := uuid.NewString()
	decidedAt := time.Date(2026, 10, 18, 10, 30, 0, 123456000, time.UTC)
	a := ReviewedEventID(donationID, decidedAt)
	b := ReviewedEventID(donationID, decidedAt)
	if a != b {
		t.Fatal("event id not deterministic")
	}
	c := ReviewedEventID(donationID, decidedAt.Add(time.Second))
	if a == c {
		t.Fatal("different decision time must produce a different event id")
	}
}