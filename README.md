# Log Pose

[![CI](https://github.com/sabure-dev/log-pose/actions/workflows/ci.yml/badge.svg)](https://github.com/sabure-dev/log-pose/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=flat&logo=python&logoColor=white)
![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.139-009688?style=flat&logo=fastapi&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244c5a?style=flat&logo=grpc&logoColor=white)
![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Jaeger-F5A800?style=flat&logo=opentelemetry&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat&logo=postgresql&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.x-FF6600?style=flat&logo=rabbitmq&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)

Polyglot microservices system (Python + Go) for order management, inventory reservation, and warehouse operations. Built on an asynchronous Choreographed Saga, Transactional Outbox, Idempotent Inbox, high-performance gRPC synchronous pre-flight checks, API Gateway perimeter security, and end-to-end distributed tracing via OpenTelemetry and Jaeger.

---

## Architecture Overview

```
                      +---------------------------------------+
                      |       Client / Frontend (HTTP)        |
                      +---------------------------------------+
                                          |
                                    Port :8080
                                          v
+-----------------------------------------------------------------------------------+
| API Gateway (`gateway_service` - Go 1.23)                                         |
|  - Auth: Register & Login, JWT issuance with roles (customer, warehouse_operator) |
|  - Perimeter Security: Strips incoming client X-* headers                         |
|  - Reverse Proxy: Injects verified X-User-Id, X-User-Role, and secret             |
|    X-Internal-Token for operators -> forwards /api/v1/orders/* to order_service   |
|  - gRPC Client: Queries inventory_service directly for catalog & stock checks     |
|  - OpenTelemetry: Starts root spans, injects W3C traceparent into HTTP and gRPC   |
+-----------------------------------------------------------------------------------+
             | (HTTP Internal :8000)                        | (gRPC Internal :50051)
             v                                              v
+--------------------------------------+     +--------------------------------------+
| Order Service (`order_service`)      |     | Inventory Service (`inventory_svc`)  |
| (Python 3.13, FastAPI, asyncpg, uv)  |     | (Go 1.23, pgx/v5, gRPC, errgroup)    |
|  - Pre-flight check via gRPC         |     |  - gRPC Server:                      |
|  - Transactional Outbox & UoW        |     |     * CheckStockAvailability         |
|  - Emits: order.created              |     |     * ListProducts                   |
|  - Consumes: inventory.reserved/fail |     |     * GetProductStock                |
|  - Saga state transitions            |     |  - AMQP Consumer:                    |
+--------------------------------------+     |     * Reserves stock (deadlock-free) |
             |                               |     * Atomic SAVEPOINT deduction     |
             |                               |  - Transactional Outbox Relay        |
             +--------------------+----------+--------------------------------------+
                                  |
                                  v
+-----------------------------------------------------------------------------------+
| RabbitMQ (logpose_events)                                                         |
|  - Topic Exchange: logpose_events                                                 |
|  - Dead Letter Exchange: logpose_dlx                                              |
|  - Delayed Retry Queues (TTL + DLX)                                               |
|  - Traceparent propagation in AMQP message headers                                |
+-----------------------------------------------------------------------------------+
                                  |
                                  v
+-----------------------------------------------------------------------------------+
| Jaeger All-in-One (`jaeger:4318` OTLP, Web UI on `http://localhost:16686`)       |
|  - Unified distributed trace waterfall:                                           |
|    Gateway HTTP -> FastAPI -> gRPC Stock Check -> AMQP Publish -> Go Consumer     |
+-----------------------------------------------------------------------------------+
```

---

## Services & Core Components

### 1. `gateway_service` (Go 1.23)
* **Auth Module (`internal/auth`):**
  * Registration & Login: `POST /api/v1/auth/register`, `POST /api/v1/auth/login`.
  * Generates signed JWT tokens with claims: `user_id`, `username`, and `role` (`customer`, `warehouse_operator`, `admin`).
  * Seeds demo accounts: `operator` / `operatorpass123`, `admin` / `adminpass123`.
* **Perimeter Security & Reverse Proxy (`internal/proxy`):**
  * Strips **all** incoming client `X-*` headers to eliminate spoofing attacks.
  * Injects verified `X-User-Id` and `X-User-Role`.
  * Automatically injects secret `X-Internal-Token` only for authorized roles (`warehouse_operator` and `admin`).
  * Seamlessly rewrites paths: `/api/v1/orders/*` -> `/orders/*`.
* **Synchronous gRPC Client (`internal/inventory`):**
  * Direct gRPC connection to `inventory_service:50051`.
  * Exposes: `GET /api/v1/inventory/products`, `GET /api/v1/inventory/products/{id}`, `POST /api/v1/inventory/check`.
* **CORS & Middleware:**
  * Handles cross-origin requests and `OPTIONS` preflight with `204 No Content`.
  * OpenTelemetry W3C `traceparent` propagation on all outbound HTTP and gRPC calls.

### 2. `order_service` (Python 3.13, FastAPI, asyncpg, uv)
* **Pre-flight Validation via gRPC:**
  * When `POST /orders/` is invoked, checks idempotency cache first to prevent false rejections on client retries.
  * Calls `GrpcInventoryClient.check_stock_availability` over gRPC.
  * Returns immediate error responses (`400 Bad Request` for unknown product or invalid quantity, `409 Conflict` for out of stock or price mismatch, `422` for unprocessable gRPC errors).
* **Transactional Outbox & Choreographed Saga:**
  * Order persistence and outbox event creation occur atomically in the same database transaction.
  * `outbox_relay` background worker publishes `order.created`, `order.cancelled`, and `order.completed` events with exponential backoff and publisher confirms.
  * `inventory_consumer` processes `inventory.reserved` and `inventory.failed` events, updating order status.

### 3. `inventory_service` (Go 1.23, pgx/v5, errgroup)
* **gRPC Server (`internal/grpcserver`):**
  * Implements `InventoryService` defined in `proto/inventory/v1/inventory.proto`.
  * Runs concurrently with RabbitMQ consumer and Outbox relay using `errgroup` and graceful shutdown.
  * Sub-millisecond read queries: stock availability and price checks in a single SQL query per item.
* **Concurrency & Deadlock Prevention:**
  * Sorts `product_id` slices before executing stock deductions to eliminate database deadlocks.
  * Protects multi-item reservations using PostgreSQL `SAVEPOINT` so partial reservations roll back cleanly on insufficient stock.
  * Idempotency table `reservations` protects against duplicate deliveries and handles tombstone cancellation records.

---

## Choreographed Saga Events (Exchange: `logpose_events`)

| Event | Routing Key | Publisher | Consumer | Description |
| :--- | :--- | :--- | :--- | :--- |
| `OrderCreated` | `order.created` | `order_service` | `inventory_service` | New order created, triggers inventory reservation |
| `OrderCancelled` | `order.cancelled` | `order_service` | `inventory_service` | Order cancelled, releases held stock reservations |
| `OrderCompleted` | `order.completed` | `order_service` | `inventory_service` | Order completed, permanently deducts reserved stock |
| `InventoryReservationResult` | `inventory.reserved` | `inventory_service` | `order_service` | Items reserved, order transitions to `reserved` |
| `InventoryReservationResult` | `inventory.failed` | `inventory_service` | `order_service` | Insufficient stock or price mismatch, order transitions to `cancelled` |

---

## Network Perimeter Security

Only public endpoints are exposed to the host machine. All internal microservice communication runs on the isolated Docker network `logpose_net`:

| Service | Internal Port | Exposed to Host | Description |
| :--- | :--- | :--- | :--- |
| **`gateway_service`** | `:8080` | **`0.0.0.0:8080`** | **Single public ingress point** |
| `order_service` | `:8000` | *None (isolated)* | Reached only via Gateway reverse proxy |
| `inventory_service` (gRPC) | `:50051` | *None (isolated)* | Reached only via Gateway & Order Service gRPC clients |
| `postgres` | `:5432` | *None (isolated)* | Reached only within Docker network |
| `rabbitmq` (AMQP) | `:5672` | *None (isolated)* | Reached only within Docker network |
| `rabbitmq` (Admin UI) | `:15672` | `127.0.0.1:15672` | Management console (debugging) |
| `jaeger` (UI) | `:16686` | `127.0.0.1:16686` | Distributed tracing visualization |

---

## Quick Start

### 1. Configure Environment
```bash
cp .env.example .env
```

### 2. Start Full Stack
```bash
docker compose up --build -d
```

### 3. Run End-to-End Smoke Test
Run the automated smoke test script that verifies the complete lifecycle through the API Gateway:
```bash
python scripts/smoke_test.py
```

The script automatically executes:
1. Health check of API Gateway (`/health`).
2. Customer registration via public `/api/v1/auth/register`.
3. Privilege escalation prevention verification (403 Forbidden) and admin provisioning of warehouse operator via `/api/v1/admin/users`.
4. Catalog browsing over gRPC via `/api/v1/inventory/products`.
5. Pre-flight stock validation over gRPC via `/api/v1/inventory/check`.
6. Order creation through Gateway with JWT authentication.
7. Polling Choreographed Saga transition (`pending` -> `reserved`).
8. Warehouse operator shipping order (`POST /api/v1/orders/{id}/ship`).
9. Warehouse operator completing order (`POST /api/v1/orders/{id}/complete`).
10. Final verification of order completion and inventory deductions.

### 4. Distributed Tracing in Jaeger
Open `http://localhost:16686` in your browser:
* View end-to-end trace waterfalls showing the complete path from the API Gateway HTTP handler to FastAPI, gRPC stock availability checks, RabbitMQ AMQP message headers, and the Go consumer.

### 5. Interactive Swagger UI & OpenAPI Specification
For manual testing and exploration, the API Gateway provides interactive Swagger UI:
* **Interactive Swagger UI:** [`http://localhost:8080/docs`](http://localhost:8080/docs) (or `http://localhost:8080/swagger`)
* **Raw OpenAPI 3.0 Spec:** [`http://localhost:8080/api/v1/openapi.json`](http://localhost:8080/api/v1/openapi.json)

Use the "Authorize" button in Swagger UI to enter your JWT token after calling `/api/v1/auth/login` or `/api/v1/auth/register`, allowing seamless manual testing of all order and warehouse fulfillment endpoints directly from your browser.

---

## API Usage via Gateway (`http://localhost:8080`)

### Authentication
```bash
# Register Customer
curl -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"password123","role":"customer"}'

# Login
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"password123"}'
```

### Browse Inventory (gRPC via Gateway)
```bash
# List Catalog
curl -X GET http://localhost:8080/api/v1/inventory/products

# Pre-flight Check
curl -X POST http://localhost:8080/api/v1/inventory/check \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {
        "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "quantity": 2,
        "expected_price": "1500.50"
      }
    ]
  }'
```

### Create Order (Reverse Proxy to `order_service`)
```bash
curl -X POST http://localhost:8080/api/v1/orders/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <CUSTOMER_JWT_TOKEN>" \
  -H "Idempotency-Key: order-req-001" \
  -d '{
    "items": [
      {
        "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "quantity": 2,
        "price": "1500.50"
      }
    ],
    "shipping_address": "42 Cloud Avenue, Tokyo"
  }'
```

### Ship & Complete Order (Operator)
```bash
# Transition to READY_FOR_SHIPPING
curl -X POST http://localhost:8080/api/v1/orders/<order_id>/ship \
  -H "Authorization: Bearer <OPERATOR_JWT_TOKEN>"

# Transition to COMPLETED
curl -X POST http://localhost:8080/api/v1/orders/<order_id>/complete \
  -H "Authorization: Bearer <OPERATOR_JWT_TOKEN>"
```

---

## Testing

### Run Gateway Service Tests (Go)
```bash
cd gateway_service
go test -count=1 ./...
```

### Run Inventory Service Tests (Go)

* **Unit Tests (fast in-memory bufconn gRPC, mock consumers, domain logic):**
  ```bash
  cd inventory_service
  go test -count=1 ./...
  ```

* **Integration Tests (PostgreSQL & RabbitMQ):**
  Integration tests verify transaction isolation, overselling locks (`FOR UPDATE`, `SAVEPOINT`), and deadlock avoidance on real PostgreSQL. The database name **must end with `_test`** (e.g. `inventory_test`, created automatically by `infra/init-db.sh`):

  ```bash
  # 1. Start test PostgreSQL container (with auto-created inventory_test via init-db.sh)
  docker run -d --name logpose_test_pg --rm \
    -e POSTGRES_USER=logpose_admin \
    -e POSTGRES_PASSWORD=logpose_password \
    -e POSTGRES_DB=logpose_db \
    -e INVENTORY_DB_PASSWORD=inventory_password \
    -v $(pwd)/infra/init-db.sh:/docker-entrypoint-initdb.d/init-db.sh:ro \
    -p 5433:5432 \
    postgres:18-alpine

  # 2. (Optional) Start test RabbitMQ
  docker run -d --name logpose_test_rmq --rm -p 5673:5672 rabbitmq:4-alpine

  # 3. Run integration tests
  cd inventory_service
  TEST_DATABASE_URL="postgres://inventory_user:inventory_password@localhost:5433/inventory_test?sslmode=disable" \
  TEST_RABBITMQ_URL="amqp://guest:guest@localhost:5673/" \
  go test -race -v -p 1 ./...
  ```

### Run Order Service Tests & Linter (Python)
```bash
cd order_service
uv run pytest
uv run ruff check .
```
