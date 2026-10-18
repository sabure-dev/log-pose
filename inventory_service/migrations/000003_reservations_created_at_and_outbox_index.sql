ALTER TABLE reservations ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS ix_inventory_outbox_processed
    ON outbox_events (created_at) WHERE processed = TRUE;
