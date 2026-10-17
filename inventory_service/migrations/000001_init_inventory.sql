CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_stocks (
    product_id UUID PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    available INT NOT NULL CHECK (available >= 0),
    reserved INT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reservations (
    order_id UUID PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'RELEASED', 'FAILED', 'CONFIRMED')),
    items JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE reservations DROP CONSTRAINT IF EXISTS reservations_status_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_status_check
    CHECK (status IN ('RESERVED', 'RELEASED', 'FAILED', 'CONFIRMED'));

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_inventory_outbox_unprocessed
    ON outbox_events (created_at) WHERE processed = FALSE;
