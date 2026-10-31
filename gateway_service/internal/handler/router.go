package handler

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

	"gateway_service/internal/auth"
	"gateway_service/internal/docs"
	"gateway_service/internal/inventory"
	"gateway_service/internal/proxy"
	"gateway_service/internal/telemetry"
	"github.com/google/uuid"
)

type RouterConfig struct {
	AuthHandler      *auth.Handler
	InventoryHandler *inventory.Handler
	OrdersProxy      *proxy.OrdersProxy
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" {
			reqID = r.Header.Get("X-Correlation-Id")
		}
		if reqID == "" {
			reqID = uuid.New().String()
		}
		w.Header().Set("X-Request-Id", reqID)

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx = telemetry.ContextWithCorrelationID(ctx, reqID)
		tracer := telemetry.Tracer("gateway-service")
		ctx, span := tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.URL.Path))
		defer span.End()

		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(rw, r.WithContext(ctx))

		duration := time.Since(start)
		span.SetAttributes(
			attribute.Int("http.status_code", rw.statusCode),
			attribute.String("http.method", r.Method),
			attribute.String("http.target", r.URL.Path),
			attribute.String("http.request_id", reqID),
		)

		slog.Info("HTTP request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.statusCode,
			"duration", duration,
			"remote", r.RemoteAddr,
			"request_id", reqID,
			"trace_id", span.SpanContext().TraceID().String(),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Correlation-Id, X-Request-Id, traceparent, Idempotency-Key")
		w.Header().Set("Access-Control-Expose-Headers", "X-Request-Id, X-Correlation-Id")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func NewRouter(cfg *RouterConfig) http.Handler {
	mux := http.NewServeMux()

	// 1. Healthcheck
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"gateway_service"}`))
	})
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"gateway_service"}`))
	})

	// 2. Auth Endpoints (Public)
	mux.HandleFunc("POST /api/v1/auth/register", cfg.AuthHandler.Register)
	mux.HandleFunc("POST /api/v1/auth/login", cfg.AuthHandler.Login)

	// Admin-only User Provisioning
	adminUserHandler := cfg.AuthHandler.AuthMiddleware(
		cfg.AuthHandler.RequireRole(auth.RoleAdmin)(http.HandlerFunc(cfg.AuthHandler.CreatePrivilegedUser)),
	)
	mux.Handle("POST /api/v1/admin/users", adminUserHandler)

	// 3. Inventory Endpoints (Public or Authenticated catalog browsing)
	mux.HandleFunc("GET /api/v1/inventory/products", cfg.InventoryHandler.ListProducts)
	mux.HandleFunc("POST /api/v1/inventory/check", cfg.InventoryHandler.CheckStockAvailability)
	mux.HandleFunc("GET /api/v1/inventory/products/", func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimPrefix(r.URL.Path, "/api/v1/inventory/products/") == "" {
			cfg.InventoryHandler.ListProducts(w, r)
		} else {
			cfg.InventoryHandler.GetProductStock(w, r)
		}
	})

	// 4. Orders Reverse Proxy (Strictly Authenticated)
	// Wraps orders proxy with JWT AuthMiddleware
	protectedOrdersProxy := cfg.AuthHandler.AuthMiddleware(cfg.OrdersProxy)
	mux.Handle("/api/v1/orders/", protectedOrdersProxy)
	mux.Handle("/api/v1/orders", protectedOrdersProxy)

	// 5. Documentation (OpenAPI specification & interactive Swagger UI)
	mux.HandleFunc("GET /openapi.json", docs.OpenAPIJSONHandler)
	mux.HandleFunc("GET /api/v1/openapi.json", docs.OpenAPIJSONHandler)
	mux.HandleFunc("GET /docs", docs.SwaggerUIHandler)
	mux.HandleFunc("GET /api/v1/docs", docs.SwaggerUIHandler)
	mux.HandleFunc("GET /swagger", docs.SwaggerUIHandler)

	return CORSMiddleware(LoggingMiddleware(mux))
}
