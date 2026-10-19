# Inventory Service

High-performance Go service for warehouse stock reservations, catalog management, and synchronous stock verification, featuring a gRPC server, Idempotent Inbox, and Transactional Outbox patterns over RabbitMQ and PostgreSQL.

## Features

* **Synchronous gRPC Server (port `50051`):**
  * Implements `InventoryService` defined in `proto/inventory/v1/inventory.proto`.
  * `CheckStockAvailability`: Synchronous pre-flight checks verifying item existence, positive quantity, decimal price matching, and available stock in a single SQL query per item.
  * `ListProducts`: Paginated warehouse catalog with available and reserved balances.
  * `GetProductStock`: Detailed balance and catalog pricing by product UUID.
  * Unary interceptor with W3C `traceparent` extraction and OpenTelemetry span instrumentation.
* **Atomic Stock Operations with `SAVEPOINT`:** Concurrency-safe reservation via row-level checks `UPDATE inventory_stocks SET available = available - $1, reserved = reserved + $1 WHERE product_id = $2 AND available >= $1`, wrapped in a transaction savepoint so partial deductions are rolled back cleanly if any subsequent item fails.
* **Deadlock Prevention:** Deterministic sorting of `product_id` slices before executing database updates.
* **Idempotent Inbox & Tombstones:** State tracking via `reservations` table (`RESERVED`, `RELEASED`, `FAILED`, `CONFIRMED`) to eliminate duplicate mutations upon message redelivery and prevent late reservations on cancelled orders.
* **Full Saga Lifecycle Handling:**
  * `order.created` — Verifies catalog existence, strict decimal price matching, and reserves stock.
  * `order.cancelled` — Releases held reservations back into available stock.
  * `order.completed` — Permanently deducts reserved stock (`CONFIRMED`).
* **Confirmed Delayed Retries & DLQ:** Uses `inventory_order_events_retry_queue` (`TTL + DLX` via default exchange with publisher confirms) and routes exhausted messages to `inventory_order_events_dlq` (bound to `inventory_order_events_dlq`, `inventory_order_events_queue`, and `order.#`).
* **Lock-Free Network I/O in Outbox Relay:** Claims event batches via short lease updates (`FOR UPDATE SKIP LOCKED` committed immediately) before publishing to RabbitMQ, with up to 20 attempts (5-minute backoff ceiling), poison-event parking, and `RequeueParkedEvents` recovery.
* **OpenTelemetry Distributed Tracing:** Automatically extracts W3C `traceparent` from gRPC metadata and RabbitMQ message headers, creating child spans with propagation across the entire asynchronous pipeline.
* **Embedded Versioned Migrations & Opt-In Seeding:** SQL migrations are embedded via `embed.FS` and applied under a PostgreSQL transaction advisory lock (`pg_advisory_xact_lock`). Demo catalog seeding runs only when `SEED_CATALOG=true`.

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DATABASE_URL` | PostgreSQL connection string (`postgres://user:pass@host:5432/inventory_db?sslmode=disable`) | *Required* |
| `RABBITMQ_URL` | RabbitMQ connection string (`amqp://user:pass@host:5672/`) | *Required* |
| `GRPC_PORT` | Port for the gRPC server | `50051` |
| `SEED_CATALOG` | Seeds demo catalog products on startup (`true`/`false`) | `false` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP endpoint for distributed traces (e.g. `http://jaeger:4318`) | *None (local)* |
| `TEST_DATABASE_URL` | Dedicated PostgreSQL URL for integration tests (database name **must** end with `_test`) | *None* |
| `TEST_RABBITMQ_URL` | Optional RabbitMQ URL for running live AMQP topology and DLQ integration tests | *None* |

## Testing

### Unit Tests
Fast unit tests using in-memory `bufconn` for gRPC handlers, mock consumers, and domain validation:
```bash
go test -count=1 ./...
```

### Integration Tests (PostgreSQL + RabbitMQ)
Integration tests verify transaction isolation, row-level locks against overselling, and deadlock avoidance on real PostgreSQL. The database name **must end with `_test`** (e.g. `inventory_test`, created automatically by `infra/init-db.sh`):

```bash
# 1. Start test PostgreSQL instance (with auto-created inventory_test)
docker run -d --name logpose_test_pg --rm \
  -e POSTGRES_USER=logpose_admin \
  -e POSTGRES_PASSWORD=logpose_password \
  -e POSTGRES_DB=logpose_db \
  -e INVENTORY_DB_PASSWORD=inventory_password \
  -v $(pwd)/../infra/init-db.sh:/docker-entrypoint-initdb.d/init-db.sh:ro \
  -p 5433:5432 \
  postgres:18-alpine

# 2. (Optional) Start test RabbitMQ
docker run -d --name logpose_test_rmq --rm -p 5673:5672 rabbitmq:4-alpine

# 3. Run integration tests
TEST_DATABASE_URL="postgres://inventory_user:inventory_password@localhost:5433/inventory_test?sslmode=disable" \
TEST_RABBITMQ_URL="amqp://guest:guest@localhost:5673/" \
go test -race -v -p 1 ./...
```
