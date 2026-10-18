# Log Pose

[![CI](https://github.com/sabure-dev/log-pose/actions/workflows/ci.yml/badge.svg)](https://github.com/sabure-dev/log-pose/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=flat&logo=python&logoColor=white)
![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.139-009688?style=flat&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat&logo=postgresql&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.x-FF6600?style=flat&logo=rabbitmq&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)

Polyglot microservices system (Python + Go) for order management and warehouse stock reservations built on an asynchronous Choreographed Saga, Transactional Outbox, and Idempotent Inbox patterns.

## Services Architecture

* **`order_service` (Python 3.13, FastAPI, SQLAlchemy 2.0 async, Alembic, `uv`):**
  * REST API for order creation (with `Idempotency-Key` deduplication and payload conflict detection), retrieval, shipping, completion, cancellation, and parked-event maintenance.
  * Designed to run behind a trusted API Gateway that authenticates requests and forwards the verified `X-User-Id` header for tenant isolation. Operator and fulfillment endpoints (`/ship`, `/complete`, and `/maintenance/requeue-parked`) strictly require the secret `X-Internal-Token` header (with optional verified `X-User-Role` of `warehouse`, `operator`, or `admin`) to prevent role-spoofing attacks.
  * Unit of Work and Transactional Outbox: atomic order persistence and event recording in PostgreSQL.
  * Background worker `outbox_relay`: claims unprocessed events via short leases (`FOR UPDATE SKIP LOCKED` committed before network I/O), publishes to RabbitMQ with exponential backoff (up to 20 attempts, 5-minute ceiling), isolates dead-lettered events (parking with alert metrics), and expires stale pending orders.
  * Background consumer `inventory_consumer`: handles inventory reservation outcomes (`RESERVED` / `FAILED` with truncated `cancellation_reason`) via a finite state machine with delayed retry queues (`TTL + DLX`) and dead-letter handling.

* **`inventory_service` (Go 1.23, `pgx/v5`, `amqp091-go`, `errgroup`):**
  * RabbitMQ consumer with manual acknowledgment, publisher-confirmed delayed retry queue (`TTL + DLX`), and Dead Letter Queue bound to both initial and retry routing keys.
  * Idempotency via `reservations` table and Inbox: protects against duplicate processing on redelivery and handles early-cancellation tombstones.
  * Atomic stock reservation with `SAVEPOINT` rollback protection and deadlock prevention by pre-sorting `product_id` slices before executing updates.
  * Catalog price verification and stock deduction (`CONFIRMED`) upon `order.completed`.
  * Transactional Outbox in Go: claims batches with short leases so no DB locks are held during network I/O, tracks confirmation per event, parks poison events, and logs alerts when intervention is required.

## Choreographed Saga Events (Exchange: `logpose_events`)

| Event | Routing Key | Publisher | Consumer | Description |
| :--- | :--- | :--- | :--- | :--- |
| `OrderCreated` | `order.created` | `order_service` | `inventory_service` | New order created, requests catalog price verification and item reservations |
| `OrderCancelled` | `order.cancelled` | `order_service` | `inventory_service` | Order cancelled, releases held stock reservations (or records tombstone) |
| `OrderCompleted` | `order.completed` | `order_service` | `inventory_service` | Order completed, permanently deducts reserved stock (`CONFIRMED`) |
| `InventoryReservationResult` | `inventory.reserved` | `inventory_service` | `order_service` | Items successfully reserved, order transitions to `reserved` |
| `InventoryReservationResult` | `inventory.failed` | `inventory_service` | `order_service` | Insufficient stock or price/catalog mismatch, order transitions to `cancelled` with `cancellation_reason` |

JSON schema contracts are versioned under `docs/contracts/`.

## Quick Start

### 1. Environment Variables

Create `.env` in the project root:

```bash
cp .env.example .env
```

(Optional, for running `order_service` outside Docker):

```bash
cp order_service/.env.example order_service/.env
```

### 2. Run with Docker Compose

```bash
docker compose up --build
```

> **Note on upgrading existing volumes:** If you previously ran an older version of the project, reset existing PostgreSQL and RabbitMQ volumes so that `infra/init-db.sh` creates `inventory_db` / `inventory_test` and RabbitMQ recreates queues with the updated `x-dead-letter-exchange` arguments (avoiding `PRECONDITION_FAILED`):

```bash
docker compose down -v
docker compose up --build
```

Service endpoints (bound to `127.0.0.1`):
* **Order API:** `http://localhost:8000` (Swagger UI: `http://localhost:8000/docs`)
* **RabbitMQ Management:** `http://localhost:15672` (default credentials from `.env`)
* **PostgreSQL:** `localhost:5433`

Default seeded catalog products in `inventory_db` (enabled by default via `SEED_CATALOG=true` in `.env.example` and `docker-compose.yml`):
* `3fa85f64-5717-4562-b3fc-2c963f66afa6` — Mechanical Keyboard (`1500.50`, stock: `100`)
* `123e4567-e89b-12d3-a456-426614174000` — Wireless Mouse (`300.00`, stock: `50`)

## API Usage

Customer order endpoints require the `X-User-Id` header. Warehouse fulfillment and operator maintenance endpoints (`/ship`, `/complete`, `/maintenance/requeue-parked`) strictly require `X-Internal-Token`.

* `POST /orders/` — Create a new order.
  * Supports optional `Idempotency-Key` header (returns `409 Conflict` if a key is reused with different parameters).
  * Items validation: 1 to 100 items per order, no duplicate `product_id`, mandatory `quantity` between 1 and 10,000, valid positive `price` (or alias `expected_price`), and order total <= 100,000,000.00.
* `GET /orders/` — List orders for the authenticated user (`skip`, `limit`).
* `GET /orders/{order_id}` — Get single order details (scoped to owner, includes `cancellation_reason` if cancelled).
* `POST /orders/{order_id}/ship` — Transition a `reserved` order to `ready_for_shipping` (requires `X-Internal-Token`; once shipping, orders cannot be cancelled by the customer).
* `POST /orders/{order_id}/complete` — Complete a `ready_for_shipping` order and deduct reserved warehouse stock (requires `X-Internal-Token`).
* `POST /orders/{order_id}/cancel` — Cancel an order (allowed only when `pending` or `reserved`; triggers compensating stock release if reserved).
* `POST /orders/maintenance/requeue-parked` — Requeue parked outbox events for retry after resolving downstream broker issues (requires `X-Internal-Token`).
* `GET /orders/healthcheck` — Service and database connectivity health check.

### Create Order Example

```bash
curl -X POST http://localhost:8000/orders/ \
  -H "Content-Type: application/json" \
  -H "X-User-Id: 123e4567-e89b-12d3-a456-426614174000" \
  -H "Idempotency-Key: order-2026-0001" \
  -d '{
    "items": [
      {
        "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "quantity": 2,
        "expected_price": "1500.50"
      }
    ],
    "shipping_address": "Main Street 100, Apt 4B"
  }'
```

### Ship & Complete Order Example (Warehouse / Operator)

```bash
curl -X POST http://localhost:8000/orders/<order_id>/ship \
  -H "X-Internal-Token: logpose-internal-secret"

curl -X POST http://localhost:8000/orders/<order_id>/complete \
  -H "X-Internal-Token: logpose-internal-secret"
```

### Cancel Order Example

```bash
curl -X POST http://localhost:8000/orders/<order_id>/cancel \
  -H "X-User-Id: 123e4567-e89b-12d3-a456-426614174000"
```

### Requeue Parked Events Example (Operator)

```bash
curl -X POST http://localhost:8000/orders/maintenance/requeue-parked \
  -H "X-Internal-Token: logpose-internal-secret"
```

## Running Tests

### Order Service (Python)

```bash
cd order_service
uv run pytest
```

### Inventory Service (Go)

Integration tests require a database name ending with `_test` (e.g. `inventory_test`, created automatically by `infra/init-db.sh`) so running tests never wipes your development `inventory_db` catalog:

```bash
# setup test rabbitmq environment
docker run -d --rm -p 5673:5672 rabbitmq:4-alpine

cd inventory_service
TEST_RABBITMQ_URL="amqp://guest:guest@localhost:5673/" \
TEST_DATABASE_URL="postgres://inventory_user:inventory_password@localhost:5433/inventory_test?sslmode=disable" \
go test -race -v -p 1 ./...
```
