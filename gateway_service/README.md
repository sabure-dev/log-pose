# API Gateway Service

High-performance Go 1.23 API Gateway serving as the single public perimeter ingress point for the Log Pose microservices platform.

## Features

* **JWT Authentication & Role-Based Access Control:**
  * Public registration and login endpoints: `POST /api/v1/auth/register`, `POST /api/v1/auth/login`.
  * Persistent PostgreSQL user store (`PostgresUserStore`) using connection pooling with automatic schema initialization and demo user seeding.
  * Graceful fallback to thread-safe in-memory store (`MemoryUserStore`) when `DATABASE_URL` is not provided (e.g., unit tests).
  * Secure bcrypt password hashing and case-insensitive username uniqueness.
  * Signed JWT tokens containing claims: `user_id`, `username`, and `role` (`customer`, `warehouse_operator`, `admin`).
  * Pre-seeded demo operator accounts:
    * `demo_customer` / `demo_password` (`customer`)
    * `demo_operator` / `demo_password` (`warehouse_operator`)
    * `demo_admin` / `demo_password` (`admin`)
    * `operator` / `operatorpass123` (`warehouse_operator`)
    * `admin` / `adminpass123` (`admin`)
* **Perimeter Security & Header Sanitization:**
  * Strips **all** incoming client `X-*` headers to eliminate header-spoofing attacks.
  * Injects cryptographically verified `X-User-Id` and `X-User-Role` from valid JWT tokens.
  * Injects the secret `X-Internal-Token` only for authorized warehouse operator and admin roles.
* **HTTP Reverse Proxy to `order_service`:**
  * Reverse proxies `/api/v1/orders/*` -> `http://order_service:8000/orders/*`.
  * Preserves path parameters, query parameters, and idempotency keys.
* **Synchronous gRPC Client to `inventory_service`:**
  * Queries `inventory_service:50051` directly over gRPC.
  * Public REST endpoints:
    * `GET /api/v1/inventory/products` — Catalog listing with pagination.
    * `GET /api/v1/inventory/products/{id}` — Detailed stock balance by product ID.
    * `POST /api/v1/inventory/check` — Pre-flight stock availability and catalog price check.
* **Interactive Documentation (Swagger UI & OpenAPI 3.0):**
  * Interactive Swagger UI available at `http://localhost:8080/docs` (or `/swagger`).
  * Raw OpenAPI 3.0.3 specification available at `http://localhost:8080/api/v1/openapi.json`.
  * Supports Swagger UI "Authorize" button to paste JWT tokens and test protected order workflows directly from the browser.
* **CORS & Middleware:**
  * Full CORS support for browser-based frontends, responding to `OPTIONS` preflight with `204 No Content`.
  * Root span creation and W3C `traceparent` injection via OpenTelemetry.

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PORT` | HTTP port for API Gateway | `8080` |
| `DATABASE_URL` | PostgreSQL connection string for persistent user store | *None (falls back to in-memory)* |
| `ORDER_SERVICE_URL` | Base URL of internal order_service | `http://localhost:8000` |
| `INVENTORY_GRPC_HOST` | Hostname of inventory gRPC service | `localhost` |
| `INVENTORY_GRPC_PORT` | Port of inventory gRPC service | `50051` |
| `INTERNAL_API_TOKEN` | Secret token injected for operator requests | `logpose-internal-secret` |
| `JWT_SECRET` | Secret key used for signing JWT tokens | `logpose-jwt-secret-key-change-in-production` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP collector endpoint | *None (local)* |

## Testing

Run all unit tests:
```bash
go test -count=1 ./...
```
