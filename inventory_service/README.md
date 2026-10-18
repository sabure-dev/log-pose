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
* **Confirmed Delayed Retries & DLQ:** Uses `inventory_order_events_retry_queue` (`TTL + DLX` via default exchange with publisher confirms) and routes exhausted messages to `inventory_order_events_dlq` (bound to `inventory_order_events_dlq`, `inventory_order_events_queue`, and `order.#`).
* **Lock-Free Network I/O in Outbox Relay:** Claims event batches via short lease updates (`FOR UPDATE SKIP LOCKED` committed immediately) before publishing to RabbitMQ, with up to 20 attempts (5-minute backoff ceiling), poison-event parking, and `RequeueParkedEvents` recovery.
* **Embedded Versioned Migrations & Opt-In Seeding:** SQL migrations are embedded via `embed.FS` and applied under a PostgreSQL transaction advisory lock (`pg_advisory_xact_lock`). Demo catalog seeding runs only when `SEED_CATALOG=true`.

## Environment Variables

| Variable | Description |
| :--- | :--- |
| `DATABASE_URL` | PostgreSQL connection string (`postgres://user:pass@host:5432/inventory_db?sslmode=disable`) |
| `RABBITMQ_URL` | RabbitMQ connection string (`amqp://user:pass@host:5672/`) |
| `SEED_CATALOG` | Optional (`true`/`false`, default `false`). Seeds demo catalog products on startup |
| `TEST_DATABASE_URL` | Dedicated PostgreSQL URL for integration tests (database name **must** end with `_test`) |
| `TEST_RABBITMQ_URL` | Optional RabbitMQ URL for running live AMQP topology and DLQ integration tests |

## Testing

```bash
# setup test rabbitmq environment
docker run -d --rm -p 5673:5672 rabbitmq:4-alpine

TEST_RABBITMQ_URL="amqp://guest:guest@localhost:5673/" \
TEST_DATABASE_URL="postgres://inventory_user:inventory_password@localhost:5433/inventory_test?sslmode=disable" \
go test -race -v -p 1 ./...
```
