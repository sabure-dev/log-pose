-- Seed default catalog items matching README and contract examples
INSERT INTO products (id, name, price)
VALUES
    ('3fa85f64-5717-4562-b3fc-2c963f66afa6', 'Mechanical Keyboard', 1500.50),
    ('123e4567-e89b-12d3-a456-426614174000', 'Wireless Mouse', 300.00)
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory_stocks (product_id, available, reserved)
VALUES
    ('3fa85f64-5717-4562-b3fc-2c963f66afa6', 100, 0),
    ('123e4567-e89b-12d3-a456-426614174000', 50, 0)
ON CONFLICT (product_id) DO NOTHING;
