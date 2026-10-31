package proxy_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"gateway_service/internal/auth"
	"gateway_service/internal/proxy"
	"github.com/google/uuid"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestOrdersProxy_HeaderStrippingAndInjection(t *testing.T) {
	const secretToken = "my-secret-internal-token"

	var receivedHeaders http.Header
	var receivedPath string
	var receivedQuery string

	mockTransport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		receivedHeaders = r.Header.Clone()
		receivedPath = r.URL.Path
		receivedQuery = r.URL.RawQuery
		if r.Body != nil {
			_, _ = io.ReadAll(r.Body)
		}

		rec := httptest.NewRecorder()
		rec.Header().Set("Content-Type", "application/json")
		rec.WriteHeader(http.StatusOK)
		_, _ = rec.Write([]byte(`{"status":"success"}`))
		resp := rec.Result()
		resp.Request = r
		return resp, nil
	})

	ordersProxy, err := proxy.NewOrdersProxyWithTransport("http://order_service:8000", secretToken, mockTransport)
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	testUserID := uuid.New()

	tests := []struct {
		name              string
		role              auth.Role
		clientHeaders     map[string]string
		expectInternalTok bool
	}{
		{
			name: "Customer gets X-User-Id but NO X-Internal-Token even if spoofed",
			role: auth.RoleCustomer,
			clientHeaders: map[string]string{
				"X-User-Id":        uuid.New().String(), // spoof attempt
				"X-Internal-Token": "hacked-token",      // spoof attempt
				"X-User-Role":      "admin",             // spoof attempt
				"X-Custom-Hack":    "malicious",
				"Idempotency-Key":  "idem-12345",
			},
			expectInternalTok: false,
		},
		{
			name: "Warehouse Operator gets verified X-User-Id and X-Internal-Token",
			role: auth.RoleWarehouseOperator,
			clientHeaders: map[string]string{
				"X-User-Id":        "fake-id",
				"X-Internal-Token": "fake-token",
				"Idempotency-Key":  "idem-operator",
			},
			expectInternalTok: true,
		},
		{
			name: "Admin gets verified X-User-Id and X-Internal-Token",
			role: auth.RoleAdmin,
			clientHeaders: map[string]string{
				"Idempotency-Key": "idem-admin",
			},
			expectInternalTok: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orders/123/ship?urgent=true", nil)
			for k, v := range tt.clientHeaders {
				req.Header.Set(k, v)
			}

			// Attach authenticated context
			claims := &auth.Claims{
				UserID:   testUserID,
				Username: "testuser",
				Role:     tt.role,
			}
			req = req.WithContext(auth.ContextWithUser(context.Background(), claims))

			w := httptest.NewRecorder()
			ordersProxy.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}

			// Verify path rewriting: /api/v1/orders/123/ship -> /orders/123/ship
			if receivedPath != "/orders/123/ship" {
				t.Fatalf("expected path /orders/123/ship, got %s", receivedPath)
			}
			if receivedQuery != "urgent=true" {
				t.Fatalf("expected query urgent=true, got %s", receivedQuery)
			}

			// Verify Idempotency-Key was preserved
			if receivedHeaders.Get("Idempotency-Key") != tt.clientHeaders["Idempotency-Key"] {
				t.Fatalf("expected Idempotency-Key preserved, got %s", receivedHeaders.Get("Idempotency-Key"))
			}

			// Verify spoofed X-Custom-Hack was stripped
			if receivedHeaders.Get("X-Custom-Hack") != "" {
				t.Fatalf("expected X-Custom-Hack stripped, got %s", receivedHeaders.Get("X-Custom-Hack"))
			}

			// Verify X-Correlation-Id and X-Request-Id were populated
			if receivedHeaders.Get("X-Correlation-Id") == "" {
				t.Fatalf("expected X-Correlation-Id to be set")
			}
			if receivedHeaders.Get("X-Request-Id") == "" {
				t.Fatalf("expected X-Request-Id to be set")
			}

			// Verify verified X-User-Id injected
			if receivedHeaders.Get("X-User-Id") != testUserID.String() {
				t.Fatalf("expected X-User-Id %s, got %s", testUserID.String(), receivedHeaders.Get("X-User-Id"))
			}

			// Verify verified X-User-Role injected
			if receivedHeaders.Get("X-User-Role") != string(tt.role) {
				t.Fatalf("expected X-User-Role %s, got %s", string(tt.role), receivedHeaders.Get("X-User-Role"))
			}

			// Verify X-Internal-Token presence
			if tt.expectInternalTok {
				if receivedHeaders.Get("X-Internal-Token") != secretToken {
					t.Fatalf("expected X-Internal-Token %s, got %s", secretToken, receivedHeaders.Get("X-Internal-Token"))
				}
			} else {
				if receivedHeaders.Get("X-Internal-Token") != "" {
					t.Fatalf("expected NO X-Internal-Token for customer, got %s", receivedHeaders.Get("X-Internal-Token"))
				}
			}
		})
	}
}
