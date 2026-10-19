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

	// 1. Create customer user
	user, err := store.CreateUser(ctx, "alice", "securepass123", auth.RoleCustomer)
	if err != nil {
		t.Fatalf("unexpected error creating user: %v", err)
	}
	if user.Username != "alice" || user.Role != auth.RoleCustomer {
		t.Fatalf("unexpected user fields: %+v", user)
	}

	// 2. Reject duplicate username
	_, err = store.CreateUser(ctx, "Alice", "anotherpass", auth.RoleCustomer)
	if err != auth.ErrUserAlreadyExists {
		t.Fatalf("expected ErrUserAlreadyExists, got %v", err)
	}

	// 3. Authenticate success
	authed, err := store.Authenticate(ctx, "alice", "securepass123")
	if err != nil {
		t.Fatalf("failed to authenticate: %v", err)
	}
	if authed.ID != user.ID {
		t.Fatalf("expected ID %s, got %s", user.ID, authed.ID)
	}

	// 4. Authenticate bad password
	_, err = store.Authenticate(ctx, "alice", "wrongpassword")
	if err != auth.ErrInvalidCredentials {
		t.Fatalf("expected ErrInvalidCredentials, got %v", err)
	}

	// 5. Authenticate nonexistent user
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

	// Expired token test
	shortTM := auth.NewTokenManager("test-secret-key-123", -1*time.Minute)
	expiredToken, err := shortTM.GenerateToken(user)
	if err != nil {
		t.Fatalf("failed to generate expired token: %v", err)
	}
	_, err = tm.VerifyToken(expiredToken)
	if err != auth.ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken for expired token, got %v", err)
	}

	// Tampered token test
	_, err = tm.VerifyToken(tokenStr + "corrupted")
	if err != auth.ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken for corrupted token, got %v", err)
	}
}

func TestAuthHandler_RegisterAndLogin(t *testing.T) {
	store := auth.NewMemoryUserStore()
	tm := auth.NewTokenManager("handler-secret-key", 1*time.Hour)
	h := auth.NewHandler(store, tm)

	// Register
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

	// Login
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

	// 1. Missing Authorization header
	reqNoAuth := httptest.NewRequest(http.MethodGet, "/test", nil)
	wNoAuth := httptest.NewRecorder()
	wrapped.ServeHTTP(wNoAuth, reqNoAuth)
	if wNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth header, got %d", wNoAuth.Code)
	}

	// 2. Invalid Bearer format
	reqBadFormat := httptest.NewRequest(http.MethodGet, "/test", nil)
	reqBadFormat.Header.Set("Authorization", "Basic invalid")
	wBadFormat := httptest.NewRecorder()
	wrapped.ServeHTTP(wBadFormat, reqBadFormat)
	if wBadFormat.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad auth format, got %d", wBadFormat.Code)
	}

	// 3. Valid Bearer Token
	reqValid := httptest.NewRequest(http.MethodGet, "/test", nil)
	reqValid.Header.Set("Authorization", "Bearer "+validToken)
	wValid := httptest.NewRecorder()
	wrapped.ServeHTTP(wValid, reqValid)
	if wValid.Code != http.StatusOK || !nextCalled {
		t.Fatalf("expected 200 and next called, got %d, nextCalled=%v", wValid.Code, nextCalled)
	}
}
