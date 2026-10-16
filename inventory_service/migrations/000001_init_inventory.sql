CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
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
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'RELEASED', 'FAILED')),
    items JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inbox_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_inventory_outbox_unprocessed 
    ON outbox_events (created_at) WHERE processed = FALSE;

INSERT INTO products (id, name, price) VALUES 
('3fa85f64-5717-4562-b3fc-2c963f66afa6', 'Mechanical Keyboard', 1500.50),
('123e4567-e89b-12d3-a456-426614174000', 'Ergonomic Mouse', 300.00)
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory_stocks (product_id, available, reserved) VALUES 
('3fa85f64-5717-4562-b3fc-2c963f66afa6', 100, 0),
('123e4567-e89b-12d3-a456-426614174000', 50, 0)
ON CONFLICT (product_id) DO NOTHING;
