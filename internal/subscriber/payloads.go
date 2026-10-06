package subscriber

import "time"

type topupCompleted struct {
	DonationID       string    `json:"donation_id"`
	IntentID         string    `json:"intent_id"`
	StreamerID       string    `json:"streamer_id"`
	AmountIDR        int64     `json:"amount_idr"`
	MDRIDR           int64     `json:"mdr_idr"`
	MDRRateBPS       int64     `json:"mdr_rate_bps"`
	GrossChargedIDR  int64     `json:"gross_charged_idr"`
	PlatformFeeIDR   int64     `json:"platform_fee_idr"`
	NetIDR           int64     `json:"net_idr"`
	Currency         string    `json:"currency"`
	Provider         string    `json:"provider"`
	PaymentMethod    string    `json:"payment_method"`
	ProviderChargeID string    `json:"provider_charge_id"`
	CapturedAt       time.Time `json:"captured_at"`
}

type topupFailed struct {
	DonationID       string    `json:"donation_id"`
	IntentID         string    `json:"intent_id"`
	StreamerID       string    `json:"streamer_id"`
	AmountIDR        int64     `json:"amount_idr"`
	Provider         string    `json:"provider"`
	ProviderChargeID string    `json:"provider_charge_id"`
	FailureReason    string    `json:"failure_reason"`
	FailedAt         time.Time `json:"failed_at"`
}

type topupExpired struct {
	DonationID       string    `json:"donation_id"`
	IntentID         string    `json:"intent_id"`
	StreamerID       string    `json:"streamer_id"`
	AmountIDR        int64     `json:"amount_idr"`
	Provider         string    `json:"provider"`
	ProviderChargeID string    `json:"provider_charge_id"`
	ExpiredAt        time.Time `json:"expired_at"`
}
