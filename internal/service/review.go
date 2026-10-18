// Package service holds streamer-facing domain rules for donation review,
// ported verbatim from inflora-front's src/models/rules (CONTRACT_DRIFT D-038).
// Owner decision 2026-10-18: these rules NEVER gate or delay overlay airing —
// the server only records the flag reason at charge so the creator dashboard
// can badge it; the creator's reject is a post-hoc takedown.
package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/google/uuid"
)

// Ported verbatim from src/models/rules/moderation.ts (BLOCKED_PHRASES).
var blockedPhrases = []string{
	"scam", "free money", "dm me", "giveaway link", "crypto x100", "click here",
}

// Ported verbatim from src/models/rules/moderation.ts (LINK_PATTERN); the
// client's /i flag applies to the whole regex, so anchor it with (?i). The
// repeat rule uses an explicit run-length scan instead: Go's RE2 cannot
// compile the client's (.)\\1{7,} backreference.
var linkPattern = regexp.MustCompile(`(?i)https?://|www\.|\b[a-z0-9-]+\.(com|net|org|io|gg|xyz|ru|ly)\b`)

// ModerationResult mirrors ModerateMessage's return in moderation.ts.
type ModerationResult struct {
	Flagged bool
	Reason  string
}

// ModerateMessage is the parity port of moderateMessage in
// inflora-front/src/models/rules/moderation.ts. Per-rule sources match the
// client: links on the RAW text (case-insensitive), blocked phrases on the
// LOWERCASED text, all-caps on the LETTERS-ONLY extraction (>= 12 letters and
// all upper), spam as a run of >= 8 consecutive identical UTF-16 code units
// on the RAW text with no case folding. Check order and reason strings are
// the client's.
func ModerateMessage(text string) ModerationResult {
	normalized := strings.ToLower(text)
	if linkPattern.MatchString(text) {
		return ModerationResult{Flagged: true, Reason: "Contains a link"}
	}
	for _, phrase := range blockedPhrases {
		if strings.Contains(normalized, phrase) {
			return ModerationResult{Flagged: true, Reason: "Blocked phrase"}
		}
	}
	var letters []byte
	for i := 0; i < len(text); i++ {
		c := text[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			letters = append(letters, c)
		}
	}
	if len(letters) >= 12 && string(letters) == strings.ToUpper(string(letters)) {
		return ModerationResult{Flagged: true, Reason: "All caps"}
	}
	if hasRepeatedRun(text, 8) {
		return ModerationResult{Flagged: true, Reason: "Spam pattern"}
	}
	return ModerationResult{}
}

// hasRepeatedRun reports whether text contains a run of at least n identical
// UTF-16 code units — the explicit scan behind the client's (.)\1{7,}
// (n = 8). JS regexes without the 'u' flag operate on UTF-16 code units and
// '.' never crosses a line terminator, so the scan decodes runes to code
// units (astral chars become two non-repeating units, exactly like JS) and
// resets the run at U+000A/U+000D/U+2028/U+2029. No case folding.
func hasRepeatedRun(text string, n int) bool {
	if n < 2 {
		return false
	}
	units := utf16.Encode([]rune(text))
	if len(units) < n {
		return false
	}
	run := 1
	for i := 1; i < len(units); i++ {
		if units[i] == units[i-1] && !isJSLineTerminator(units[i]) {
			run++
			if run >= n {
				return true
			}
			continue
		}
		run = 1
	}
	return false
}

// isJSLineTerminator mirrors the four code points the JS '.' wildcard never
// matches: LF, CR, U+2028 LINE SEPARATOR, U+2029 PARAGRAPH SEPARATOR.
func isJSLineTerminator(u uint16) bool {
	return u == 0x000A || u == 0x000D || u == 0x2028 || u == 0x2029
}

// FlagReasonForMessage returns the dashboard flag reason for a donation
// message, or "" when the message is clean (or absent). The charge path
// stores it on the donation; it never affects airing.
func FlagReasonForMessage(message string) string {
	result := ModerateMessage(message)
	if result.Flagged {
		return result.Reason
	}
	return ""
}

// Decision errors surfaced by ApplyDecision; the HTTP layer maps each to a
// status code via errors.Is. ErrReviewNotFound doubles as the streamer-ownership
// miss so a caller cannot probe other creators' donations.
var (
	ErrReviewNotFound    = errors.New("review: donation not found")
	ErrReviewBadDecision = errors.New("review: invalid decision")
	ErrReviewNotAired    = errors.New("review: donation never aired")
	ErrReviewTerminal    = errors.New("review: rejection is terminal")
)

const (
	ReviewApprove = "APPROVED"
	ReviewReject  = "REJECTED"
)

// ReviewOutcome reports what ApplyDecision did. Changed is false when the
// decision was already applied (idempotent re-delivery from the dashboard).
type ReviewOutcome struct {
	DonationID string
	Decision   string
	DecidedAt  time.Time
	Changed    bool
}

// reviewDonation is the CAS row the decision path needs: the review fields
// plus the display subset required to build donation.reviewed.v1 payloads.
// AiredAt coalesces review_aired_at to charged_at so rows charged before
// migration 00011 are handled identically.
type reviewDonation struct {
	ID, IntentID, StreamerID, Status, ReviewStatus, SettlementStatus, PaymentMethod string
	AmountIDR, MDRIDR, GrossChargedIDR, PlatformFeeIDR, NetIDR                     int64
	DonorDisplayName                                                                string
	IsAnonymous                                                                     bool
	Message, VoiceURL, YouTubeURL                                                   string
	VoiceDurationSec, YouTubeStartSec, YouTubeEndSec, DisplayDurationSec            int
	FlagReason                                                                      sql.NullString
	AiredAt                                                                         time.Time
	DecidedAt                                                                       sql.NullTime
}

func loadReviewDonation(ctx context.Context, tx *sql.Tx, id uuid.UUID) (reviewDonation, error) {
	var d reviewDonation
	err := tx.QueryRowContext(ctx, `SELECT d.id,d.intent_id,d.streamer_id,COALESCE(d.status::text,''),COALESCE(d.review_status::text,''),d.amount_idr,COALESCE(d.mdr_idr,0),COALESCE(d.gross_charged_idr,0),COALESCE(d.platform_fee_idr,0),COALESCE(d.net_idr,0),COALESCE(d.donor_display_name,''),d.is_anonymous,COALESCE(d.message,''),COALESCE(d.voice_url,''),COALESCE(d.voice_duration_sec,0),COALESCE(d.youtube_url,''),COALESCE(d.youtube_start_sec,0),COALESCE(d.youtube_end_sec,0),COALESCE(d.display_duration_sec,0),COALESCE(d.settlement_status::text,''),COALESCE(d.payment_method::text,''),d.review_flag_reason,COALESCE(d.review_aired_at,d.charged_at),d.review_decided_at FROM donations d WHERE d.id=$1 FOR UPDATE`, id).
		Scan(&d.ID, &d.IntentID, &d.StreamerID, &d.Status, &d.ReviewStatus, &d.AmountIDR, &d.MDRIDR, &d.GrossChargedIDR, &d.PlatformFeeIDR, &d.NetIDR, &d.DonorDisplayName, &d.IsAnonymous, &d.Message, &d.VoiceURL, &d.VoiceDurationSec, &d.YouTubeURL, &d.YouTubeStartSec, &d.YouTubeEndSec, &d.DisplayDurationSec, &d.SettlementStatus, &d.PaymentMethod, &d.FlagReason, &d.AiredAt, &d.DecidedAt)
	return d, err
}

// ApplyDecision applies a creator review decision on an already-aired
// donation (CONTRACT_DRIFT D-038, post-hoc semantics):
//
//   - REJECTED flips the row REJECTED, records the decision and enqueues
//     donation.reviewed.v1 in the SAME transaction (transactional outbox), so
//     the gateway can replay-safely take down the already-aired frame. Once
//     rejected the decision is terminal — approve never restores.
//   - APPROVED is an acknowledgment: it stamps review_decided_at so the row
//     leaves the dashboard queue, and never emits an event (the charged event
//     is the only airing signal). A late APPROVED after REJECTED is a conflict.
//
// Ownership is enforced here, not only at the facade: the streamer must match
// the donation row. Re-delivering an already-applied decision is idempotent.
func ApplyDecision(ctx context.Context, db *sql.DB, donationID, streamerID uuid.UUID, decision string) (ReviewOutcome, error) {
	decision = strings.ToUpper(strings.TrimSpace(decision))
	if decision != ReviewApprove && decision != ReviewReject {
		return ReviewOutcome{}, ErrReviewBadDecision
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ReviewOutcome{}, err
	}
	defer tx.Rollback()

	d, err := loadReviewDonation(ctx, tx, donationID)
	if errors.Is(err, sql.ErrNoRows) {
		return ReviewOutcome{}, ErrReviewNotFound
	}
	if err != nil {
		return ReviewOutcome{}, fmt.Errorf("review: load donation: %w", err)
	}
	if d.StreamerID != streamerID.String() {
		return ReviewOutcome{}, ErrReviewNotFound
	}
	if d.Status != "CHARGED" && d.Status != "SETTLED" && d.Status != "REFUNDED" {
		return ReviewOutcome{}, ErrReviewNotAired
	}
	out := ReviewOutcome{DonationID: d.ID, Decision: decision}
	if d.ReviewStatus == ReviewReject && decision == ReviewApprove {
		return ReviewOutcome{}, ErrReviewTerminal
	}
	// Idempotency: a re-delivered REJECTED matches the stored status; a
	// re-delivered APPROVED matches the stored acknowledgment (charged rows
	// already read APPROVED, so the marker for "creator acted" is decided_at).
	if (decision == ReviewReject && d.ReviewStatus == ReviewReject) || (decision == ReviewApprove && d.DecidedAt.Valid) {
		out.DecidedAt = d.DecidedAt.Time
		return out, nil
	}

	var decidedAt time.Time
	if decision == ReviewReject {
		if err := tx.QueryRowContext(ctx, `UPDATE donations SET review_status='REJECTED',review_decided_at=NOW(),review_actor='streamer' WHERE id=$1 RETURNING review_decided_at`, donationID).Scan(&decidedAt); err != nil {
			return ReviewOutcome{}, err
		}
		if err := enqueueReviewed(ctx, tx, d, decidedAt); err != nil {
			return ReviewOutcome{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE donations SET review_event_published_at=$2 WHERE id=$1`, donationID, decidedAt); err != nil {
			return ReviewOutcome{}, err
		}
	} else {
		if err := tx.QueryRowContext(ctx, `UPDATE donations SET review_decided_at=NOW(),review_actor='streamer' WHERE id=$1 RETURNING review_decided_at`, donationID).Scan(&decidedAt); err != nil {
			return ReviewOutcome{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ReviewOutcome{}, err
	}
	out.DecidedAt = decidedAt
	out.Changed = true
	return out, nil
}

// ReviewedEventID derives the deterministic event id for a rejection decision.
// Reconciliation re-enqueues the SAME id, so JetStream Nats-Msg-Id dedupe and
// every consumer inbox treat a resurrected delivery as the original event.
func ReviewedEventID(donationID string, decidedAt time.Time) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("donation.reviewed.v1:"+donationID+":"+decidedAt.UTC().Format(time.RFC3339Nano)))
}

// enqueueReviewed writes the donation.reviewed.v1 takedown event into the
// transactional outbox. The display subset mirrors DonationChargedEvent so
// consumers can act without a DB read.
func enqueueReviewed(ctx context.Context, tx *sql.Tx, d reviewDonation, decidedAt time.Time) error {
	eventID := ReviewedEventID(d.ID, decidedAt)
	payload := events.DonationReviewedEvent{
		DonationID: d.ID, IntentID: d.IntentID, StreamerID: d.StreamerID,
		ReviewStatus: ReviewReject, Reviewer: "streamer",
		FlagReason:       d.FlagReason.String,
		ReviewedAt:       decidedAt,
		AiredAt:          &d.AiredAt,
		AmountIDR:        d.AmountIDR,
		MDRIDR:           d.MDRIDR,
		GrossChargedIDR:  d.GrossChargedIDR,
		PlatformFeeIDR:   d.PlatformFeeIDR,
		NetIDR:           d.NetIDR,
		DonorDisplayName: d.DonorDisplayName,
		IsAnonymous:      d.IsAnonymous,
		Message:          d.Message,
		VoiceURL:         d.VoiceURL,
		VoiceDurationSec: d.VoiceDurationSec,
		YouTubeURL:       d.YouTubeURL,
		YouTubeStartSec:  d.YouTubeStartSec,
		YouTubeEndSec:    d.YouTubeEndSec,
		DisplayDurationSec: d.DisplayDurationSec,
		SettlementStatus: d.SettlementStatus,
		PaymentMethod:    d.PaymentMethod,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	env := events.Envelope{
		EventID: eventID.String(), EventType: events.DonationReviewed, EventVersion: "1",
		OccurredAt: decidedAt, Producer: "saruman", StreamerID: &d.StreamerID, Payload: raw,
	}
	rawEnv, err := json.Marshal(env)
	if err != nil {
		return err
	}
	// Idempotent insert: the deterministic event id makes a heal-lane
	// re-enqueue (or any re-delivered decide transaction) collide on
	// event_id and become a no-op instead of a unique violation.
	_, err = outbox.EnqueueRawIdempotent(ctx, tx, outbox.RawParams{
		EventID: eventID, Producer: "saruman", EventType: env.EventType, EventVersion: env.EventVersion,
		AggregateType: "donation", AggregateID: uuid.MustParse(d.ID),
		StreamerID: sql.NullString{String: d.StreamerID, Valid: true},
		Subject:    events.Subject(env.EventType, d.StreamerID),
		Envelope:   rawEnv, OccurredAt: decidedAt, AvailableAt: decidedAt,
	})
	return err
}