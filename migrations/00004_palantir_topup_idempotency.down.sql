DROP INDEX IF EXISTS uq_gateway_topups_provider_charge;
DROP INDEX IF EXISTS uq_gateway_topups_provider_request;

ALTER TABLE gateway_topups
  DROP COLUMN IF EXISTS expires_at,
  DROP COLUMN IF EXISTS payment_url,
  DROP COLUMN IF EXISTS request_hash,
  DROP COLUMN IF EXISTS provider_request_id;