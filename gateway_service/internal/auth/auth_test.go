package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gateway_service/internal/auth"
	"github.com/google/uuid"
)

func TestUserStore_CreateAndAuthenticate(t *testing.T) {
	store := auth.NewMemoryUserStore()
	ctx := context.Background()

	user, err := store.CreateUser(ctx, "alice", "securepass123", auth.RoleCustomer)
	if err != nil {
		t.Fatalf("unexpected error creating user: %v", err)
	}
	if user.Username != "alice" || user.Role != auth.RoleCustomer {
		t.Fatalf("unexpected user fields: %+v", user)
	}

	_, err = store.CreateUser(ctx, "Alice", "anotherpass", auth.RoleCustomer)
	if err != auth.ErrUserAlreadyExists {
		t.Fatalf("expected ErrUserAlreadyExists, got %v", err)
	}

	authed, err := store.Authenticate(ctx, "alice", "securepass123")
	if err != nil {
		t.Fatalf("failed to authenticate: %v", err)
	}
	if authed.ID != user.ID {
		t.Fatalf("expected ID %s, got %s", user.ID, authed.ID)
	}

	_, err = store.Authenticate(ctx, "alice", "wrongpassword")
	if err != auth.ErrInvalidCredentials {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}

	_, err = store.Authenticate(ctx, "nonexistent", "pass")
	if err != auth.ErrInvalidCredentials {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestTokenManager_Lifecycle(t *testing.T) {
	tm := auth.NewTokenManager("test-secret-key-123", 1*time.Hour)
	user := &auth.User{
		ID:       uuid.New(),
		Username: "operator_bob",
		Role:     auth.RoleWarehouseOperator,
	}

	tokenStr, err := tm.GenerateToken(user)
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	claims, err := tm.VerifyToken(tokenStr)
	if err != nil {
		t.Fatalf("failed to verify token: %v", err)
	}
	if claims.UserID != user.ID || claims.Username != "operator_bob" || claims.Role != auth.RoleWarehouseOperator {
		t.Fatalf("claims mismatch: %+v", claims)
	}

	shortTM := auth.NewTokenManager("test-secret-key-123", -1*time.Minute)
	expiredToken, err := shortTM.GenerateToken(user)
	if err != nil {
		t.Fatalf("failed to generate expired token: %v", err)
	}
	_, err = tm.VerifyToken(expiredToken)
	if err != auth.ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken for expired token, got %v", err)
	}

	_, err = tm.VerifyToken(tokenStr + "corrupted")
	if err != auth.ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken for corrupted token, got %v", err)
	}
}

func TestAuthHandler_RegisterAndLogin(t *testing.T) {
	store := auth.NewMemoryUserStore()
	tm := auth.NewTokenManager("handler-secret-key", 1*time.Hour)
	h := auth.NewHandler(store, tm)

	regBody, _ := json.Marshal(auth.RegisterRequest{
		Username: "newuser",
		Password: "password123",
		Role:     "customer",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(regBody))
	w := httptest.NewRecorder()
	h.Register(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d: %s", w.Code, w.Body.String())
	}
	var regResp auth.AuthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &regResp); err != nil {
		t.Fatalf("failed to decode register response: %v", err)
	}
	if regResp.Token == "" || regResp.Username != "newuser" {
		t.Fatalf("invalid register response: %+v", regResp)
	}

	loginBody, _ := json.Marshal(auth.LoginRequest{
		Username: "newuser",
		Password: "password123",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(loginBody))
	loginW := httptest.NewRecorder()
	h.Login(loginW, loginReq)

	if loginW.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d: %s", loginW.Code, loginW.Body.String())
	}
	var loginResp auth.AuthResponse
	if err := json.Unmarshal(loginW.Body.Bytes(), &loginResp); err != nil {
		t.Fatalf("failed to decode login response: %v", err)
	}
	if loginResp.UserID != regResp.UserID {
		t.Fatalf("expected user ID %s, got %s", regResp.UserID, loginResp.UserID)
	}
}

func TestAuthMiddleware(t *testing.T) {
	store := auth.NewMemoryUserStore()
	tm := auth.NewTokenManager("middleware-secret-key", 1*time.Hour)
	h := auth.NewHandler(store, tm)

	user := &auth.User{
		ID:       uuid.New(),
		Username: "charlie",
		Role:     auth.RoleAdmin,
	}
	validToken, _ := tm.GenerateToken(user)

	nextCalled := false
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.UserFromContext(r.Context())
		if !ok || claims == nil {
			t.Fatalf("claims missing from request context")
		}
		if claims.UserID != user.ID || claims.Role != auth.RoleAdmin {
			t.Fatalf("claims mismatch in context: %+v", claims)
		}
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})

	wrapped := h.AuthMiddleware(testHandler)

	reqNoAuth := httptest.NewRequest(http.MethodGet, "/test", nil)
	wNoAuth := httptest.NewRecorder()
	wrapped.ServeHTTP(wNoAuth, reqNoAuth)
	if wNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth header, got %d", wNoAuth.Code)
	}

	reqBadFormat := httptest.NewRequest(http.MethodGet, "/test", nil)
	reqBadFormat.Header.Set("Authorization", "Basic invalid")
	wBadFormat := httptest.NewRecorder()
	wrapped.ServeHTTP(wBadFormat, reqBadFormat)
	if wBadFormat.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad auth format, got %d", wBadFormat.Code)
	}

	reqValid := httptest.NewRequest(http.MethodGet, "/test", nil)
	reqValid.Header.Set("Authorization", "Bearer "+validToken)
	wValid := httptest.NewRecorder()
	wrapped.ServeHTTP(wValid, reqValid)
	if wValid.Code != http.StatusOK || !nextCalled {
		t.Fatalf("expected 200 and next called, got %d, nextCalled=%v", wValid.Code, nextCalled)
	}
}

func TestAuthHandler_Register_PrivilegedRoleForbidden(t *testing.T) {
	store := auth.NewMemoryUserStore()
	tm := auth.NewTokenManager("handler-secret-key", 1*time.Hour)
	h := auth.NewHandler(store, tm)

	operBody, _ := json.Marshal(auth.RegisterRequest{
		Username: "rogue_operator",
		Password: "password123",
		Role:     "warehouse_operator",
	})
	reqOper := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(operBody))
	wOper := httptest.NewRecorder()
	h.Register(wOper, reqOper)

	if wOper.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden when self-registering as warehouse_operator, got %d: %s", wOper.Code, wOper.Body.String())
	}

	adminBody, _ := json.Marshal(auth.RegisterRequest{
		Username: "rogue_admin",
		Password: "password123",
		Role:     "admin",
	})
	reqAdmin := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(adminBody))
	wAdmin := httptest.NewRecorder()
	h.Register(wAdmin, reqAdmin)

	if wAdmin.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden when self-registering as admin, got %d: %s", wAdmin.Code, wAdmin.Body.String())
	}

	custBody, _ := json.Marshal(auth.RegisterRequest{
		Username: "plain_customer",
		Password: "password123",
	})
	reqCust := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(custBody))
	wCust := httptest.NewRecorder()
	h.Register(wCust, reqCust)

	if wCust.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", wCust.Code, wCust.Body.String())
	}
	var resp auth.AuthResponse
	_ = json.Unmarshal(wCust.Body.Bytes(), &resp)
	if resp.Role != auth.RoleCustomer {
		t.Fatalf("expected role customer, got %s", resp.Role)
	}
}

func TestAuthHandler_CreatePrivilegedUser_And_RequireRole(t *testing.T) {
	store := auth.NewMemoryUserStore()
	tm := auth.NewTokenManager("handler-secret-key", 1*time.Hour)
	h := auth.NewHandler(store, tm)

	adminEndpoint := h.RequireRole(auth.RoleAdmin)(http.HandlerFunc(h.CreatePrivilegedUser))

	newOperBody, _ := json.Marshal(auth.RegisterRequest{
		Username: "provisioned_operator",
		Password: "operatorpass123",
		Role:     "warehouse_operator",
	})

	reqUnauth := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", bytes.NewReader(newOperBody))
	wUnauth := httptest.NewRecorder()
	adminEndpoint.ServeHTTP(wUnauth, reqUnauth)
	if wUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", wUnauth.Code)
	}

	customerClaims := &auth.Claims{
		UserID:   uuid.New(),
		Username: "some_customer",
		Role:     auth.RoleCustomer,
	}
	reqCust := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", bytes.NewReader(newOperBody))
	reqCust = reqCust.WithContext(auth.ContextWithUser(reqCust.Context(), customerClaims))
	wCust := httptest.NewRecorder()
	adminEndpoint.ServeHTTP(wCust, reqCust)
	if wCust.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for customer, got %d", wCust.Code)
	}

	adminClaims := &auth.Claims{
		UserID:   uuid.New(),
		Username: "superadmin",
		Role:     auth.RoleAdmin,
	}
	reqAdmin := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", bytes.NewReader(newOperBody))
	reqAdmin = reqAdmin.WithContext(auth.ContextWithUser(reqAdmin.Context(), adminClaims))
	wAdmin := httptest.NewRecorder()
	adminEndpoint.ServeHTTP(wAdmin, reqAdmin)
	if wAdmin.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for admin provisioning, got %d: %s", wAdmin.Code, wAdmin.Body.String())
	}

	authOper, err := store.Authenticate(context.Background(), "provisioned_operator", "operatorpass123")
	if err != nil {
		t.Fatalf("failed to authenticate provisioned operator: %v", err)
	}
	if authOper.Role != auth.RoleWarehouseOperator {
		t.Fatalf("expected role warehouse_operator, got %s", authOper.Role)
	}
}
