# Order Service

FastAPI-based order management service implementing clean architecture, Transactional Outbox pattern, and asynchronous event publishing via RabbitMQ.

## Features

* **Strict Dependency Inversion:** Service layer depends on abstractions (`AbstractUnitOfWork`, `AbstractOrderRepository`), decoupled from transport schemas and ORM entities.
* **Transactional Outbox:** Order creation and event emissions are persisted atomically within PostgreSQL transactions.
* **Deterministic Status Transitions:** Finite State Machine governing `pending`, `reserved`, `ready_for_shipping`, `completed`, and `cancelled`.
* **Tenant Isolation:** Enforced `X-User-Id` header on all order endpoints with user-scoped queries.
* **Background Workers:**
  * `app.workers.outbox_relay`: Polls `outbox_events` table using `FOR UPDATE SKIP LOCKED` and publishes to RabbitMQ.
  * `app.workers.inventory_consumer`: Consumes `inventory.reserved` and `inventory.failed` events to update order states.

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USER` | Database username | `orders_user` |
| `DB_PASS` | Database password | `orders_password` |
| `DB_NAME` | Database name | `orders_db` |
| `RABBITMQ_URL` | AMQP connection string | `amqp://guest:guest@localhost:5672/` |

## Testing

```bash
uv run pytest
```
