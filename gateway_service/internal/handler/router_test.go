package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"gateway_service/internal/auth"
	"gateway_service/internal/handler"
	"gateway_service/internal/inventory"
	pb "gateway_service/internal/pb/inventoryv1"
	"gateway_service/internal/proxy"
	"gateway_service/internal/telemetry"
)

type mockInventoryGRPC struct {
	pb.UnimplementedInventoryServiceServer
}

func (m *mockInventoryGRPC) ListProducts(ctx context.Context, req *pb.ListProductsRequest, opts ...grpc.CallOption) (*pb.ListProductsResponse, error) {
	return &pb.ListProductsResponse{
		Products: []*pb.GetProductStockResponse{
			{
				ProductId: uuid.New().String(),
				Name:      "Test Item",
				Price:     "100.00",
				Available: 10,
			},
		},
		Total: 1,
	}, nil
}

func (m *mockInventoryGRPC) CheckStockAvailability(ctx context.Context, req *pb.CheckStockAvailabilityRequest, opts ...grpc.CallOption) (*pb.CheckStockAvailabilityResponse, error) {
	return &pb.CheckStockAvailabilityResponse{
		Eligible:      true,
		OverallStatus: pb.AvailabilityStatus_AVAILABILITY_STATUS_OK,
	}, nil
}

func (m *mockInventoryGRPC) GetProductStock(ctx context.Context, req *pb.GetProductStockRequest, opts ...grpc.CallOption) (*pb.GetProductStockResponse, error) {
	return &pb.GetProductStockResponse{
		ProductId: req.GetProductId(),
		Name:      "Test Item",
		Price:     "100.00",
		Available: 10,
	}, nil
}

type capturedRequest struct {
	Header http.Header
	Method string
	Path   string
	Body   []byte
}

type fakeOrderTransport struct {
	captured *capturedRequest
	response *http.Response
}

func (f *fakeOrderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	f.captured = &capturedRequest{
		Header: req.Header.Clone(),
		Method: req.Method,
		Path:   req.URL.Path,
		Body:   body,
	}

	if f.response != nil {
		return f.response, nil
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"status":"ok","id":"test-order-id"}`)),
	}, nil
}

func setupTestRouter(t *testing.T) (http.Handler, *auth.TokenManager, *fakeOrderTransport) {
	t.Helper()

	_, _ = telemetry.InitTracer(context.Background(), "gateway-test", "")

	store := auth.NewMemoryUserStore()
	tm := auth.NewTokenManager("test-jwt-secret-key-123456789012", 24*time.Hour)
	authHandler := auth.NewHandler(store, tm)

	invClient := inventory.NewClientWithService(&mockInventoryGRPC{})
	invHandler := inventory.NewHandler(invClient)

	fakeTransport := &fakeOrderTransport{}
	ordersProxy, err := proxy.NewOrdersProxyWithTransport(
		"http://order_service:8000",
		"test-internal-token",
		fakeTransport,
	)
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	r := handler.NewRouter(&handler.RouterConfig{
		AuthHandler:      authHandler,
		InventoryHandler: invHandler,
		OrdersProxy:      ordersProxy,
	})

	return r, tm, fakeTransport
}

func TestHealthcheck(t *testing.T) {
	r, _, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "gateway_service") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestCORS_PreflightOptions(t *testing.T) {
	r, _, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/orders", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content for OPTIONS, got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("missing Access-Control-Allow-Origin header")
	}
	if !strings.Contains(w.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("missing Authorization in Allow-Headers: %s", w.Header().Get("Access-Control-Allow-Headers"))
	}
}

func TestAuth_RegisterAndLoginFlow(t *testing.T) {
	r, _, _ := setupTestRouter(t)

	// Register customer
	regPayload := `{"username":"newcustomer","password":"secretpassword","role":"customer"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader(regPayload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on register, got %d: %s", w.Code, w.Body.String())
	}

	var authResp auth.AuthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &authResp); err != nil {
		t.Fatalf("failed to decode auth response: %v", err)
	}
	if authResp.Token == "" || authResp.Role != auth.RoleCustomer {
		t.Fatalf("unexpected auth response: %+v", authResp)
	}

	// Login with correct credentials
	loginPayload := `{"username":"newcustomer","password":"secretpassword"}`
	loginReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(loginPayload))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	r.ServeHTTP(loginW, loginReq)

	if loginW.Code != http.StatusOK {
		t.Fatalf("expected 200 on login, got %d", loginW.Code)
	}
}

func TestOrdersProxy_UnauthorizedWithoutToken(t *testing.T) {
	r, _, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orders", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized without token, got %d", w.Code)
	}
}

func TestOrdersProxy_CustomerToken_InjectsUserId_StripsClientHeaders(t *testing.T) {
	r, tm, transport := setupTestRouter(t)

	custUser := &auth.User{
		ID:       uuid.New(),
		Username: "cust1",
		Role:     auth.RoleCustomer,
	}
	token, err := tm.GenerateToken(custUser)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orders/", bytes.NewReader([]byte(`{"items":[]}`)))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	// Spoofed client headers that MUST be stripped
	req.Header.Set("X-User-Id", uuid.New().String())
	req.Header.Set("X-User-Role", "admin")
	req.Header.Set("X-Internal-Token", "fake-token")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from proxied order, got %d: %s", w.Code, w.Body.String())
	}

	if transport.captured == nil {
		t.Fatalf("expected request to reach fake transport")
	}

	// Verify path rewritten
	if transport.captured.Path != "/orders/" {
		t.Fatalf("expected rewritten path /orders/, got %s", transport.captured.Path)
	}

	// Verify X-User-Id matches the verified JWT user, not spoofed
	if transport.captured.Header.Get("X-User-Id") != custUser.ID.String() {
		t.Fatalf("expected X-User-Id=%s, got %s", custUser.ID, transport.captured.Header.Get("X-User-Id"))
	}
	if transport.captured.Header.Get("X-User-Role") != "customer" {
		t.Fatalf("expected X-User-Role=customer, got %s", transport.captured.Header.Get("X-User-Role"))
	}
	// For customer role, X-Internal-Token must NOT be present
	if transport.captured.Header.Get("X-Internal-Token") != "" {
		t.Fatalf("expected no X-Internal-Token for customer, got %s", transport.captured.Header.Get("X-Internal-Token"))
	}
	// Traceparent must be present
	if transport.captured.Header.Get("traceparent") == "" {
		t.Fatalf("expected traceparent header injected")
	}
}

func TestOrdersProxy_OperatorToken_InjectsInternalToken(t *testing.T) {
	r, tm, transport := setupTestRouter(t)

	opUser := &auth.User{
		ID:       uuid.New(),
		Username: "operator1",
		Role:     auth.RoleWarehouseOperator,
	}
	token, err := tm.GenerateToken(opUser)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orders/123/ship", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if transport.captured.Header.Get("X-Internal-Token") != "test-internal-token" {
		t.Fatalf("expected X-Internal-Token=test-internal-token, got %s", transport.captured.Header.Get("X-Internal-Token"))
	}
	if transport.captured.Header.Get("X-User-Role") != "warehouse_operator" {
		t.Fatalf("expected X-User-Role=warehouse_operator, got %s", transport.captured.Header.Get("X-User-Role"))
	}
}

func TestInventoryCatalog_PublicEndpoint(t *testing.T) {
	r, _, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/inventory/products", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from catalog, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Test Item") {
		t.Fatalf("expected Test Item in body: %s", w.Body.String())
	}
}

func TestDocumentation_OpenAPIAndSwagger(t *testing.T) {
	r, _, _ := setupTestRouter(t)

	// Test OpenAPI JSON
	reqJSON := httptest.NewRequest(http.MethodGet, "/api/v1/openapi.json", nil)
	wJSON := httptest.NewRecorder()
	r.ServeHTTP(wJSON, reqJSON)

	if wJSON.Code != http.StatusOK {
		t.Fatalf("expected 200 for openapi.json, got %d", wJSON.Code)
	}
	if !strings.Contains(wJSON.Body.String(), `"openapi": "3.0.3"`) {
		t.Fatalf("expected openapi spec in body: %s", wJSON.Body.String()[:100])
	}
	if wJSON.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("unexpected Content-Type: %s", wJSON.Header().Get("Content-Type"))
	}

	// Test Swagger UI HTML
	reqHTML := httptest.NewRequest(http.MethodGet, "/docs", nil)
	wHTML := httptest.NewRecorder()
	r.ServeHTTP(wHTML, reqHTML)

	if wHTML.Code != http.StatusOK {
		t.Fatalf("expected 200 for /docs, got %d", wHTML.Code)
	}
	if !strings.Contains(wHTML.Body.String(), "swagger-ui") {
		t.Fatalf("expected swagger-ui in html body")
	}
	if wHTML.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("unexpected Content-Type: %s", wHTML.Header().Get("Content-Type"))
	}
}
