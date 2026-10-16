# Log Pose

[![CI](https://github.com/sabure-dev/log-pose/actions/workflows/ci.yml/badge.svg)](https://github.com/sabure-dev/log-pose/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=flat&logo=python&logoColor=white)
![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.139-009688?style=flat&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat&logo=postgresql&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.x-FF6600?style=flat&logo=rabbitmq&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)

Polyglot microservices system (Python + Go) for order management and warehouse stock reservations built on an asynchronous Choreographed Saga, Transactional Outbox, and Idempotent Inbox patterns.

## Services Architecture

* **`order_service` (Python 3.13, FastAPI, SQLAlchemy 2.0 async, Alembic, `uv`):**
  * REST API for order creation, retrieval, and cancellation with strict tenant isolation via `X-User-Id`.
  * Unit of Work and Transactional Outbox: atomic order persistence and event recording in PostgreSQL.
  * Background worker `outbox_relay`: polls unprocessed events with `FOR UPDATE SKIP LOCKED` and publishes to RabbitMQ.
  * Background consumer `inventory_consumer`: handles inventory responses and updates order status via a finite state machine.

* **`inventory_service` (Go 1.23, `pgx/v5`, `amqp091-go`, `errgroup`):**
  * RabbitMQ consumer with manual acknowledgment (`manual ack/nack`) and QoS prefetch control.
  * Idempotency via `reservations` table and Inbox: protects against duplicate processing on redelivery.
  * Atomic stock reservation with deadlock prevention by pre-sorting `product_id` slices before executing updates.
  * Transactional Outbox in Go: records reservation outcomes atomically and publishes via background outbox relay.

## Choreographed Saga Events (Exchange: `logpose_events`)

| Event | Routing Key | Publisher | Consumer | Description |
| :--- | :--- | :--- | :--- | :--- |
| `OrderCreated` | `order.created` | `order_service` | `inventory_service` | New order created, requests item reservations |
| `OrderCancelled` | `order.cancelled` | `order_service` | `inventory_service` | Order cancelled, releases held stock reservations |
| `InventoryReservationResult` | `inventory.reserved` | `inventory_service` | `order_service` | Items successfully reserved, order transitions to `reserved` |
| `InventoryReservationResult` | `inventory.failed` | `inventory_service` | `order_service` | Insufficient stock, order transitions to `cancelled` |

JSON schema contracts are versioned under `docs/contracts/`.

## Quick Start

### 1. Environment Variables

Create `.env` in the project root:

```bash
cp .env.example .env
```

Create `.env` in `order_service/`:

```bash
cp order_service/.env.example order_service/.env
```

### 2. Run with Docker Compose

```bash
docker compose up --build
```

If initializing a fresh environment or applying updated database scripts, reset existing volumes:

```bash
docker compose down -v
docker compose up --build
```

Service endpoints:
* **Order API:** `http://localhost:8000` (Swagger UI: `http://localhost:8000/docs`)
* **RabbitMQ Management:** `http://localhost:15672` (default credentials from `.env`)
* **PostgreSQL:** `localhost:5433`

## API Usage

All order endpoints require the `X-User-Id` header.

* `POST /orders/` — Create a new order.
* `GET /orders/` — List orders for the authenticated user (`skip`, `limit`).
* `GET /orders/{order_id}` — Get single order details (scoped to owner).
* `POST /orders/{order_id}/cancel` — Cancel an order (triggers compensating stock release if reserved).
* `GET /orders/healthcheck` — Service health check.

### Create Order Example

```bash
curl -X POST http://localhost:8000/orders/ \
  -H "Content-Type: application/json" \
  -H "X-User-Id: 123e4567-e89b-12d3-a456-426614174000" \
  -d '{
    "items": [
      {
        "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "quantity": 2,
        "price": "1500.50"
      }
    ],
    "shipping_address": "Main Street 100, Apt 4B"
  }'
```

### Cancel Order Example

```bash
curl -X POST http://localhost:8000/orders/3fa85f64-5717-4562-b3fc-2c963f66afa6/cancel \
  -H "X-User-Id: 123e4567-e89b-12d3-a456-426614174000"
```

## Running Tests

### Order Service (Python)

```bash
cd order_service
uv run pytest
```

### Inventory Service (Go)

```bash
cd inventory_service
go test -v ./...
```
