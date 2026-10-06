ALTER TABLE gateway_topups
  ADD COLUMN provider_request_id VARCHAR(128),
  ADD COLUMN request_hash CHAR(64),
  ADD COLUMN payment_url TEXT NOT NULL DEFAULT '',
  ADD COLUMN expires_at TIMESTAMPTZ;

CREATE UNIQUE INDEX uq_gateway_topups_provider_request
  ON gateway_topups (provider_name, provider_request_id)
  WHERE provider_request_id IS NOT NULL;

CREATE UNIQUE INDEX uq_gateway_topups_provider_charge
  ON gateway_topups (provider_name, provider_charge_id);