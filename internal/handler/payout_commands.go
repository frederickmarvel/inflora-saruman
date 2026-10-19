package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/service"
	"github.com/google/uuid"
)

// PayoutCommandHandler exposes POST /v1/internal/payout-commands — the ONLY
// entry point into the payout machine (tolkien proxies session-authenticated
// requests here; operator scripts drive the admin commands over the same
// shared INTERNAL_API_KEY). Trust boundary (design F6, WIRE_GUIDE s5): the
// shared key is a documented shared-secret; ak_-gated admin routing arrives
// with Phase-A RBAC (00015). Money only ever moves to the streamer's own
// balance/bank, so the exposure is bounded.
type PayoutCommandHandler struct {
	DB          *sql.DB
	InternalKey string
	Lease       time.Duration
}

type payoutCommandEnvelope struct {
	Command        string          `json:"command"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	RequestID      string          `json:"request_id"`
	Actor          struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Role string `json:"role"`
	} `json:"actor"`
}

func (h *PayoutCommandHandler) Handle(w http.ResponseWriter, r *http.Request) {
	if h.InternalKey == "" || r.Header.Get("X-Internal-Api-Key") != h.InternalKey {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]string{"code": "INVALID_TOKEN", "message": "invalid internal credential"}})
		return
	}
	var env payoutCommandEnvelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "VALIDATION_FAILED", "message": "invalid json"}})
		return
	}
	if env.RequestID == "" {
		if id := r.Header.Get("X-Request-ID"); id != "" {
			env.RequestID = id
		}
	}
	status, body, err := h.dispatch(r, env)
	if err != nil {
		var code, message string
		switch {
		case errors.Is(err, service.ErrPayoutNotFound):
			code, message = "PAYOUT_NOT_FOUND", "payout not found"
		case errors.Is(err, service.ErrPayoutBankNotFound):
			code, message = "BANK_ACCOUNT_NOT_FOUND", "bank account not found"
		case errors.Is(err, service.ErrPayoutBatchNotFound):
			code, message = "BATCH_NOT_FOUND", "batch not found"
		case errors.Is(err, service.ErrPayoutIdempotencyConflict):
			code, message = "IDEMPOTENCY_CONFLICT", "idempotency key reused with a different payload"
		case errors.Is(err, service.ErrPayoutGatewayPending):
			code, message = "GATEWAY_PENDING", "gateway withdrawal row missing for this payout"
		case errors.Is(err, service.ErrPayoutIllegalTransition):
			code, message = "ILLEGAL_TRANSITION", "payout cannot move from its current state"
		case errors.Is(err, service.ErrPayoutBatchIllegalState):
			code, message = "STATE_CONFLICT", "batch cannot move from its current state"
		case errors.Is(err, service.ErrPayoutInsufficientBalance):
			code, message = "INSUFFICIENT_BALANCE", "available balance is below the requested amount"
		case errors.Is(err, service.ErrPayoutBelowMinimum):
			code, message = "BELOW_MIN_WITHDRAWAL", "amount is below the minimum withdrawal"
		case errors.Is(err, service.ErrPayoutHoldActive):
			code, message = "STREAMER_HOLD_ACTIVE", "an active fund hold blocks withdrawals"
		case errors.Is(err, service.ErrPayoutValidation):
			code, message = "VALIDATION_FAILED", "invalid command payload"
		default:
			code, message = "INTERNAL", "internal server error"
		}
		switch code {
		case "PAYOUT_NOT_FOUND", "BANK_ACCOUNT_NOT_FOUND", "BATCH_NOT_FOUND":
			status = http.StatusNotFound
		case "IDEMPOTENCY_CONFLICT", "GATEWAY_PENDING", "ILLEGAL_TRANSITION", "STATE_CONFLICT":
			status = http.StatusConflict
		case "INSUFFICIENT_BALANCE", "BELOW_MIN_WITHDRAWAL":
			status = http.StatusUnprocessableEntity
		case "VALIDATION_FAILED":
			status = http.StatusBadRequest
		case "STREAMER_HOLD_ACTIVE":
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}
		if errors.Is(err, service.ErrPayoutBankNotVerified) {
			status = http.StatusBadRequest
			code, message = "VALIDATION_FAILED", "bank account must be verified"
		}
		body = map[string]any{"error": map[string]string{"code": code, "message": message}}
	}
	writeJSON(w, status, body)
}

// dispatch routes a command envelope to the payout machine. Returns the
// status/body for success paths; errors bubble up for the shared mapping.
func (h *PayoutCommandHandler) dispatch(r *http.Request, env payoutCommandEnvelope) (int, any, error) {
	ctx := r.Context()
	lease := h.Lease
	switch env.Command {
	case "CreatePayout":
		var p struct {
			StreamerID     string `json:"streamer_id"`
			AmountIDR      int64  `json:"amount_idr"`
			BankAccountID  string `json:"bank_account_id"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		streamerID, err := uuid.Parse(p.StreamerID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		bankID, err := uuid.Parse(p.BankAccountID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		key := env.IdempotencyKey
		if key == "" {
			key = p.IdempotencyKey
		}
		var requestID uuid.UUID
		if env.RequestID != "" {
			if parsed, err := uuid.Parse(env.RequestID); err == nil {
				requestID = parsed
			}
		}
		out, err := service.CreatePayout(ctx, h.DB, service.CreatePayoutRequest{
			StreamerID: streamerID, AmountIDR: p.AmountIDR, BankAccountID: bankID,
			IdempotencyKey: key, RequestID: requestID,
		})
		if err != nil {
			return 0, nil, err
		}
		return http.StatusCreated, map[string]any{
			"payout_id": out.PayoutID.String(), "amount_idr": out.AmountIDR,
			"status": out.Status, "requested_at": out.RequestedAt, "replay": out.Replay,
		}, nil
	case "SubmitPayout":
		var p struct {
			PayoutID string `json:"payout_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		payoutID, err := uuid.Parse(p.PayoutID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		attempts, expires, err := service.SubmitPayout(ctx, h.DB, payoutID, lease)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"payout_id": payoutID.String(), "attempts": attempts, "lease_expires_at": expires}, nil
	case "CreateBatch":
		var p struct {
			CreatedBy string   `json:"created_by"`
			PayoutIDs []string `json:"payout_ids"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		createdBy, err := uuid.Parse(p.CreatedBy)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		ids := make([]uuid.UUID, 0, len(p.PayoutIDs))
		for _, s := range p.PayoutIDs {
			id, err := uuid.Parse(s)
			if err != nil {
				return 0, nil, service.ErrPayoutValidation
			}
			ids = append(ids, id)
		}
		out, err := service.CreateBatch(ctx, h.DB, createdBy, ids)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusCreated, map[string]any{
			"batch_id": out.BatchID.String(), "payout_count": out.PayoutCount, "total_amount_idr": out.TotalAmountIDR,
		}, nil
	case "AddToBatch":
		var p struct {
			BatchID   string   `json:"batch_id"`
			PayoutIDs []string `json:"payout_ids"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		batchID, err := uuid.Parse(p.BatchID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		ids := make([]uuid.UUID, 0, len(p.PayoutIDs))
		for _, s := range p.PayoutIDs {
			id, err := uuid.Parse(s)
			if err != nil {
				return 0, nil, service.ErrPayoutValidation
			}
			ids = append(ids, id)
		}
		total, count, err := service.AddToBatch(ctx, h.DB, batchID, ids)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"batch_id": batchID.String(), "payout_count": count, "total_amount_idr": total}, nil
	case "RemoveFromBatch":
		var p struct {
			BatchID  string `json:"batch_id"`
			PayoutID string `json:"payout_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		batchID, err := uuid.Parse(p.BatchID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		payoutID, err := uuid.Parse(p.PayoutID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		if err := service.RemoveFromBatch(ctx, h.DB, batchID, payoutID); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"batch_id": batchID.String(), "payout_id": payoutID.String(), "status": "REQUESTED"}, nil
	case "CancelBatch":
		var p struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		batchID, err := uuid.Parse(p.BatchID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		if err := service.CancelBatch(ctx, h.DB, batchID); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"batch_id": batchID.String(), "cancelled": true}, nil
	case "SubmitBatch":
		var p struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		batchID, err := uuid.Parse(p.BatchID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		if err := service.SubmitBatch(ctx, h.DB, batchID, lease); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"batch_id": batchID.String(), "status": "EXECUTING"}, nil
	case "CloseBatch":
		var p struct {
			BatchID        string `json:"batch_id"`
			ResolutionNote string `json:"resolution_note"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		batchID, err := uuid.Parse(p.BatchID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		if err := service.CloseBatch(ctx, h.DB, batchID, p.ResolutionNote); err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"batch_id": batchID.String(), "closed": true}, nil
	case "RetryPayout":
		var p struct {
			PayoutID string `json:"payout_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		payoutID, err := uuid.Parse(p.PayoutID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		attempts, expires, err := service.RetryPayout(ctx, h.DB, payoutID, lease)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"payout_id": payoutID.String(), "attempts": attempts, "lease_expires_at": expires}, nil
	case "ForceFailPayout":
		var p struct {
			PayoutID string `json:"payout_id"`
			Reason   string `json:"reason"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		payoutID, err := uuid.Parse(p.PayoutID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		out, err := service.ForceFailPayout(ctx, h.DB, payoutID, p.Reason)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"payout_id": payoutID.String(), "status": out.Status}, nil
	case "RefreshProviderState":
		var p struct {
			PayoutID string `json:"payout_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		payoutID, err := uuid.Parse(p.PayoutID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		out, err := service.RefreshProviderState(ctx, h.DB, payoutID)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"payout_id": payoutID.String(), "status": out.Status, "changed": out.Changed}, nil
	case "ResolvePayout":
		var p struct {
			PayoutID  string `json:"payout_id"`
			ResolveTo string `json:"resolve_to"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		payoutID, err := uuid.Parse(p.PayoutID)
		if err != nil {
			return 0, nil, service.ErrPayoutValidation
		}
		out, err := service.ResolvePayout(ctx, h.DB, payoutID, p.ResolveTo)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, map[string]any{"payout_id": payoutID.String(), "status": out.Status, "changed": out.Changed}, nil
	default:
		return http.StatusBadRequest, map[string]any{"error": map[string]string{"code": "VALIDATION_FAILED", "message": "unknown command"}}, nil
	}
}