# Order Service

FastAPI-based order management service implementing clean architecture, Transactional Outbox pattern, and asynchronous event publishing via RabbitMQ.

## Features

* **Strict Dependency Inversion:** Service layer depends on abstractions (`AbstractUnitOfWork`, `AbstractOrderRepository`), decoupled from transport schemas and ORM entities.
* **Idempotent Order Creation with Strict Validation:** Supports optional `Idempotency-Key` header on `POST /orders/` with composite `(user_id, idempotency_key)` uniqueness, blank-key normalization, and `409 Conflict` when a key is reused with different order parameters. Validates 1 to 100 items per order, required `quantity` (1 to 10,000), positive price, and duplicate `product_id` rejection.
* **Transactional Outbox with Short-Lease Claiming:** Order creation, cancellation, and completion events are persisted atomically and published by `outbox_relay` using short leases (no DB locks held during network I/O), up to 20 retry attempts (5-minute ceiling), poison-event parking with alert logging, operator manual recovery via `POST /orders/maintenance/requeue-parked`, and bounded batch cleanup of old processed events.
* **Complete Finite State Machine & Operator Security:** Governs transitions across `pending`, `reserved`, `ready_for_shipping`, `completed`, and `cancelled` (with 500-char truncated `cancellation_reason` and automatic expiration of stale `pending` orders). Customer cancellations are forbidden once shipping begins. Fulfillment transitions (`/ship` and `/complete`) and maintenance operations require secret operator token authorization (`X-Internal-Token`).

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USER` | Database username | `orders_user` |
| `DB_PASS` | Database password | `orders_password` |
| `DB_NAME` | Database name | `orders_db` |
| `RABBITMQ_URL` | AMQP connection string | `amqp://guest:guest@localhost:5672/` |
| `PENDING_ORDER_TIMEOUT_SECONDS` | Timeout before pending orders auto-expire | `900` |
| `INTERNAL_API_TOKEN` | Secret token for operator endpoints (`/ship`, `/complete`, `/maintenance/requeue-parked`) | `logpose-internal-secret` |

## Testing

```bash
uv run pytest
```
