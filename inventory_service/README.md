# Inventory Service

High-performance Go service for warehouse stock reservations and catalog management, featuring Idempotent Inbox and Transactional Outbox patterns over RabbitMQ and PostgreSQL.

## Features

* **Atomic Stock Operations with `SAVEPOINT`:** Concurrency-safe reservation via row-level checks `UPDATE inventory_stocks SET available = available - $1, reserved = reserved + $1 WHERE product_id = $2 AND available >= $1`, wrapped in a transaction savepoint so partial deductions are rolled back cleanly if any item fails.
* **Deadlock Prevention:** Deterministic sorting of `product_id` slices before executing database modifications.
* **Idempotent Inbox & Tombstones:** State tracking via `reservations` table (`RESERVED`, `RELEASED`, `FAILED`, `CONFIRMED`) to eliminate duplicate mutations upon message redelivery and prevent late reservations on cancelled orders.
* **Full Lifecycle Handling:**
  * `order.created` — Verifies catalog existence, strict decimal price matching, and reserves stock.
  * `order.cancelled` — Releases held reservations back into available stock.
  * `order.completed` — Deducts reserved stock permanently (`CONFIRMED`).
* **Delayed Retries & DLQ:** Uses `inventory_order_events_retry_queue` (`TTL + DLX` via default exchange) for exponential backoff without broadcasting duplicates to other consumers, and routes exhausted messages to `inventory_order_events_dlq`.
* **Transactional Outbox with Poison-Event Parking:** Reservation results are persisted within the same database transaction and published via background relay with per-event confirmation commits, retry backoff (`attempts`, `last_error`, `next_retry_at`), and automatic parking after max attempts.
* **Embedded Versioned Migrations:** SQL migrations and catalog seeds are embedded via `embed.FS` and applied under a PostgreSQL transaction advisory lock (`pg_advisory_xact_lock`).

## Environment Variables

| Variable | Description |
| :--- | :--- |
| `DATABASE_URL` | PostgreSQL connection string (`postgres://user:pass@host:5432/inventory_db?sslmode=disable`) |
| `RABBITMQ_URL` | RabbitMQ connection string (`amqp://user:pass@host:5672/`) |
| `TEST_DATABASE_URL` | Dedicated PostgreSQL connection string used exclusively by integration tests |

## Testing

```bash
TEST_DATABASE_URL="postgres://inventory_user:inventory_password@localhost:5433/inventory_db?sslmode=disable" go test -race -v ./...
```
