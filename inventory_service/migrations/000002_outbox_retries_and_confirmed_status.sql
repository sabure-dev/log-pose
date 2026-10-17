ALTER TABLE reservations DROP CONSTRAINT IF EXISTS reservations_status_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_status_check
    CHECK (status IN ('RESERVED', 'RELEASED', 'FAILED', 'CONFIRMED'));

ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;
