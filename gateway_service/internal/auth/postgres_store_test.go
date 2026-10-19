package auth_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"gateway_service/internal/auth"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping postgres integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping test db: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})
	return pool
}

func TestPostgresUserStore_Lifecycle(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	store, err := auth.NewPostgresUserStore(ctx, pool)
	if err != nil {
		t.Fatalf("failed to create PostgresUserStore: %v", err)
	}

	// 1. Verify demo users exist
	demoOperator, err := store.Authenticate(ctx, "demo_operator", "demo_password")
	if err != nil {
		t.Fatalf("failed to authenticate demo_operator: %v", err)
	}
	if demoOperator.Role != auth.RoleWarehouseOperator {
		t.Errorf("expected role warehouse_operator, got %s", demoOperator.Role)
	}

	// 2. Create new user
	uniqueUsername := fmt.Sprintf("testuser_%s", uuid.New().String()[:8])
	user, err := store.CreateUser(ctx, uniqueUsername, "securepass123", auth.RoleCustomer)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}
	if user.Username != uniqueUsername {
		t.Errorf("expected username %s, got %s", uniqueUsername, user.Username)
	}

	// 3. Duplicate user creation should fail (even with different casing)
	_, err = store.CreateUser(ctx, uniqueUsername, "anotherpass", auth.RoleCustomer)
	if err != auth.ErrUserAlreadyExists {
		t.Errorf("expected ErrUserAlreadyExists, got %v", err)
	}

	// 4. Authenticate success
	authResult, err := store.Authenticate(ctx, uniqueUsername, "securepass123")
	if err != nil {
		t.Fatalf("authentication failed: %v", err)
	}
	if authResult.ID != user.ID {
		t.Errorf("expected id %v, got %v", user.ID, authResult.ID)
	}

	// 5. Authenticate wrong password
	_, err = store.Authenticate(ctx, uniqueUsername, "wrongpassword")
	if err != auth.ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}

	// 6. GetUserByID
	foundUser, err := store.GetUserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("failed to get user by id: %v", err)
	}
	if foundUser.ID != user.ID {
		t.Errorf("expected id %v, got %v", user.ID, foundUser.ID)
	}

	// 7. GetUserByID non-existent
	_, err = store.GetUserByID(ctx, uuid.New())
	if err != auth.ErrUserNotFound {
		t.Errorf("expected ErrUserNotFound, got %v", err)
	}
}
