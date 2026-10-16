# Inventory Service

High-performance Go service for warehouse stock reservations and catalog management, featuring Idempotent Inbox and Transactional Outbox patterns over RabbitMQ and PostgreSQL.

## Features

* **Atomic Stock Operations:** Concurrency-safe reservation via row-level checks `UPDATE inventory_stocks SET available = available - $1, reserved = reserved + $1 WHERE product_id = $2 AND available >= $1`.
* **Deadlock Prevention:** Deterministic sorting of `product_id` slices before executing database modifications.
* **Idempotent Inbox:** State tracking via `reservations` table to eliminate duplicate mutations upon message redelivery.
* **Compensating Transactions:** Handles `order.cancelled` events by safely releasing held reservations back into available stock.
* **Transactional Outbox:** Reservation results are persisted within the same database transaction and published via background relay with AMQP `mandatory=true`.
* **Structured Observability:** Native JSON structured logging with Go's `log/slog` and correlation ID propagation.

## Environment Variables

| Variable | Description |
| :--- | :--- |
| `DATABASE_URL` | PostgreSQL connection string (`postgres://user:pass@host:5432/inventory_db?sslmode=disable`) |
| `RABBITMQ_URL` | RabbitMQ connection string (`amqp://user:pass@host:5672/`) |

Both variables are strictly required. The service exits immediately upon startup if either is missing.

## Testing

```bash
go test -v ./...
```
