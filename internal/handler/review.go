package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/frederickmarvel/inflora-saruman/internal/service"
	"github.com/google/uuid"
)

// ReviewHandler exposes the creator review decision endpoint. Tolkien proxies
// session-authenticated creator requests here; the shared INTERNAL_API_KEY
// (X-Internal-Api-Key header per D-010) is the only accepted credential.
type ReviewHandler struct {
	DB          *sql.DB
	InternalKey string
}

func (h *ReviewHandler) Review(w http.ResponseWriter, r *http.Request) {
	if h.InternalKey == "" || r.Header.Get("X-Internal-Api-Key") != h.InternalKey {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]string{"code": "INVALID_TOKEN", "message": "invalid internal credential"}})
		return
	}
	donationID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]string{"code": "NOT_FOUND", "message": "donation not found"}})
		return
	}
	var body struct {
		Decision   string `json:"decision"`
		StreamerID string `json:"streamer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "VALIDATION_FAILED", "message": "invalid json"}})
		return
	}
	streamerID, err := uuid.Parse(body.StreamerID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "VALIDATION_FAILED", "message": "streamer_id must be a UUID"}})
		return
	}
	outcome, err := service.ApplyDecision(r.Context(), h.DB, donationID, streamerID, body.Decision)
	switch {
	case errors.Is(err, service.ErrReviewNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]string{"code": "NOT_FOUND", "message": "donation not found"}})
		return
	case errors.Is(err, service.ErrReviewBadDecision):
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": map[string]string{"code": "INVALID_DECISION", "message": "decision must be APPROVED or REJECTED"}})
		return
	case errors.Is(err, service.ErrReviewNotAired):
		writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]string{"code": "NOT_AIRED", "message": "this donation never aired"}})
		return
	case errors.Is(err, service.ErrReviewTerminal):
		writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]string{"code": "TERMINAL_REJECTION", "message": "a rejected donation cannot be restored"}})
		return
	case err != nil:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]string{"code": "INTERNAL", "message": "internal server error"}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"donation_id":  outcome.DonationID,
		"decision":     outcome.Decision,
		"changed":      outcome.Changed,
		"decided_at":   outcome.DecidedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}