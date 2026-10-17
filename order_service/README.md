# Order Service

FastAPI-based order management service implementing clean architecture, Transactional Outbox pattern, and asynchronous event publishing via RabbitMQ.

## Features

* **Strict Dependency Inversion:** Service layer depends on abstractions (`AbstractUnitOfWork`, `AbstractOrderRepository`), decoupled from transport schemas and ORM entities.
* **Idempotent Order Creation:** Supports optional `Idempotency-Key` header on `POST /orders/` with composite `(user_id, idempotency_key)` uniqueness.
* **Transactional Outbox with Poison-Event Parking:** Order creation, cancellation, and completion events are persisted atomically and published by `outbox_relay` with per-event confirmation, retry backoff (`attempts`, `last_error`, `next_retry_at`), and automatic cleanup of old processed events.
* **Complete Finite State Machine:** Governs transitions across `pending`, `reserved`, `ready_for_shipping`, `completed`, and `cancelled` (with persisted `cancellation_reason` and automatic expiration of stale `pending` orders).
* **Tenant Isolation:** Enforced `X-User-Id` header on all order endpoints with user-scoped queries (designed to operate behind a trusted API Gateway).
* **Background Workers:**
  * `app.workers.outbox_relay`: Polls `outbox_events` using `FOR UPDATE SKIP LOCKED`, publishes to RabbitMQ with timeouts, and expires stale pending orders.
  * `app.workers.inventory_consumer`: Consumes `inventory.reserved` and `inventory.failed` events with structured JSON logging, `correlation_id` propagation, delayed retry queue (`TTL + DLX`), and Dead Letter Queue.

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

## Testing

```bash
uv run pytest
```
