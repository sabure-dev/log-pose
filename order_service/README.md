# Order Service

FastAPI-based order management service implementing clean architecture, Transactional Outbox pattern, and asynchronous event publishing via RabbitMQ.

## Features

* **Strict Dependency Inversion:** Service layer depends on abstractions (`AbstractUnitOfWork`, `AbstractOrderRepository`), decoupled from transport schemas and ORM entities.
* **Idempotent Order Creation with Conflict Detection:** Supports optional `Idempotency-Key` header on `POST /orders/` with composite `(user_id, idempotency_key)` uniqueness, blank-key normalization, and `409 Conflict` when a key is reused with different order parameters.
* **Transactional Outbox with Short-Lease Claiming:** Order creation, cancellation, and completion events are persisted atomically and published by `outbox_relay` using short leases (no DB locks held during network I/O), up to 20 retry attempts (5-minute ceiling), `requeue_parked_outbox_events` recovery, and bounded batch cleanup of old processed events.
* **Complete Finite State Machine & Role Separation:** Governs transitions across `pending`, `reserved`, `ready_for_shipping`, `completed`, and `cancelled` (with 500-char truncated `cancellation_reason` and automatic expiration of stale `pending` orders). Fulfillment transitions (`/ship` and `/complete`) require operator authorization (`X-Internal-Token` or `X-User-Role: warehouse`).

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
| `INTERNAL_API_TOKEN` | Secret token for operator endpoints (`/ship`, `/complete`) | `logpose-internal-secret` |

## Testing

```bash
uv run pytest
```
