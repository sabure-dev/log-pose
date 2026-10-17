package repository_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
	"inventory_service/migrations"
)

const testDBAdvisoryLockID = 74920183

func assertTestDatabaseName(t *testing.T, rawURL string) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("invalid TEST_DATABASE_URL: %v", err)
	}
	dbName := strings.TrimPrefix(parsed.Path, "/")
	if !strings.HasSuffix(dbName, "_test") {
		t.Fatalf("refusing to run destructive integration tests on non-test database %q (database name must end with _test)", dbName)
	}
}

func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping postgres integration test")
	}
	assertTestDatabaseName(t, dbURL)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}

	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		t.Fatalf("failed to acquire lock connection: %v", err)
	}
	if _, err := lockConn.Exec(ctx, "SELECT pg_advisory_lock($1)", testDBAdvisoryLockID); err != nil {
		lockConn.Release()
		pool.Close()
		t.Fatalf("failed to acquire db advisory lock: %v", err)
	}

	if err := migrations.Apply(ctx, pool); err != nil {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testDBAdvisoryLockID)
		lockConn.Release()
		pool.Close()
		t.Fatalf("failed to apply migrations: %v", err)
	}

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events; DELETE FROM reservations; DELETE FROM inventory_stocks; DELETE FROM products;")
	if err != nil {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testDBAdvisoryLockID)
		lockConn.Release()
		pool.Close()
		t.Fatalf("failed to clean tables: %v", err)
	}

	t.Cleanup(func() {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testDBAdvisoryLockID)
		lockConn.Release()
		pool.Close()
	})
	return pool
}

func seedProduct(t *testing.T, pool *pgxpool.Pool, productID uuid.UUID, price string, available int) {
	t.Helper()
	repo := repository.NewInventoryRepository(pool)
	if err := repo.UpsertProduct(context.Background(), productID, "Test Item", price, available); err != nil {
		t.Fatalf("failed to seed product via UpsertProduct: %v", err)
	}
}

func TestIsValidPriceValidation_Unit(t *testing.T) {
	repo := repository.NewInventoryRepository(nil)
	err := repo.Reserve(context.Background(), domain.OrderCreatedEvent{
		OrderID: uuid.Nil,
	})
	if !errors.Is(err, repository.ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent for nil OrderID, got %v", err)
	}
}

func TestReserve_NoOversell_200ConcurrentGoroutines(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	productID := uuid.New()
	seedProduct(t, pool, productID, "100.00", 50)

	var wg sync.WaitGroup
	var succeeded atomic.Int64
	var outOfStock atomic.Int64

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := domain.OrderCreatedEvent{
				EventID:       uuid.New(),
				CorrelationID: uuid.NewString(),
				EventType:     "OrderCreated",
				OrderID:       uuid.New(),
				UserID:        uuid.New(),
				TotalAmount:   "100.00",
				Items: []domain.OrderItemPayload{
					{
						ProductID: productID,
						Quantity:  1,
						Price:     "100.00",
					},
				},
			}
			err := repo.Reserve(ctx, ev)
			if err == nil {
				succeeded.Add(1)
			} else if errors.Is(err, repository.ErrOutOfStock) {
				outOfStock.Add(1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if succeeded.Load() != 50 {
		t.Fatalf("expected exactly 50 successful reservations, got %d", succeeded.Load())
	}
	if outOfStock.Load() != 150 {
		t.Fatalf("expected 150 out-of-stock rejections, got %d", outOfStock.Load())
	}

	var available, reserved int
	err := pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", productID).Scan(&available, &reserved)
	if err != nil {
		t.Fatalf("failed to query stock: %v", err)
	}
	if available != 0 || reserved != 50 {
		t.Fatalf("expected available=0, reserved=50, got available=%d, reserved=%d", available, reserved)
	}
}

func TestReserveFailed_ThenCancel_DoesNotInflateOrLeakStock(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	idLow := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	idHigh := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	cases := []struct {
		name       string
		keyboardID uuid.UUID
		mouseID    uuid.UUID
	}{
		{
			name:       "sufficient_first_then_insufficient_second",
			keyboardID: idLow,
			mouseID:    idHigh,
		},
		{
			name:       "insufficient_first_then_sufficient_second",
			keyboardID: idHigh,
			mouseID:    idLow,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, "DELETE FROM outbox_events; DELETE FROM reservations; DELETE FROM inventory_stocks; DELETE FROM products;")
			if err != nil {
				t.Fatalf("failed to clean tables: %v", err)
			}

			seedProduct(t, pool, tc.keyboardID, "1500.50", 100)
			seedProduct(t, pool, tc.mouseID, "300.00", 50)

			orderID := uuid.New()
			createEv := domain.OrderCreatedEvent{
				EventID:     uuid.New(),
				EventType:   "OrderCreated",
				OrderID:     orderID,
				UserID:      uuid.New(),
				TotalAmount: "301500.50",
				Items: []domain.OrderItemPayload{
					{ProductID: tc.keyboardID, Quantity: 1, Price: "1500.50"},
					{ProductID: tc.mouseID, Quantity: 1000, Price: "300.00"},
				},
			}

			err = repo.Reserve(ctx, createEv)
			if !errors.Is(err, repository.ErrOutOfStock) {
				t.Fatalf("expected ErrOutOfStock, got %v", err)
			}

			cancelEv := domain.OrderCancelledEvent{
				EventID:   uuid.New(),
				EventType: "OrderCancelled",
				OrderID:   orderID,
				UserID:    createEv.UserID,
				Items:     createEv.Items,
			}
			if err := repo.Release(ctx, cancelEv); err != nil {
				t.Fatalf("expected nil on release of failed order, got %v", err)
			}

			var kbAvail, kbRes, mouseAvail, mouseRes int
			_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", tc.keyboardID).Scan(&kbAvail, &kbRes)
			_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", tc.mouseID).Scan(&mouseAvail, &mouseRes)

			if kbAvail != 100 || kbRes != 0 {
				t.Fatalf("expected keyboard 100/0, got %d/%d", kbAvail, kbRes)
			}
			if mouseAvail != 50 || mouseRes != 0 {
				t.Fatalf("expected mouse 50/0, got %d/%d", mouseAvail, mouseRes)
			}
		})
	}
}

func TestCancelBeforeReserve_TombstonePreventsLateReservation(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	productID := uuid.New()
	seedProduct(t, pool, productID, "50.00", 10)
	orderID := uuid.New()

	cancelEv := domain.OrderCancelledEvent{
		EventID:   uuid.New(),
		EventType: "OrderCancelled",
		OrderID:   orderID,
		UserID:    uuid.New(),
		Items: []domain.OrderItemPayload{
			{ProductID: productID, Quantity: 3},
		},
	}
	if err := repo.Release(ctx, cancelEv); err != nil {
		t.Fatalf("expected nil on early release (tombstone), got %v", err)
	}

	createEv := domain.OrderCreatedEvent{
		EventID:     uuid.New(),
		EventType:   "OrderCreated",
		OrderID:     orderID,
		UserID:      cancelEv.UserID,
		TotalAmount: "150.00",
		Items: []domain.OrderItemPayload{
			{ProductID: productID, Quantity: 3, Price: "50.00"},
		},
	}
	err := repo.Reserve(ctx, createEv)
	if !errors.Is(err, repository.ErrAlreadyProcessed) {
		t.Fatalf("expected ErrAlreadyProcessed due to tombstone, got %v", err)
	}

	var available, reserved int
	_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", productID).Scan(&available, &reserved)
	if available != 10 || reserved != 0 {
		t.Fatalf("expected stock untouched (10/0), got %d/%d", available, reserved)
	}
}

func TestReserve_ThenRelease_RestoresStockAndIsIdempotent(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	productID := uuid.New()
	seedProduct(t, pool, productID, "250.00", 20)
	orderID := uuid.New()

	createEv := domain.OrderCreatedEvent{
		EventID:     uuid.New(),
		EventType:   "OrderCreated",
		OrderID:     orderID,
		UserID:      uuid.New(),
		TotalAmount: "1000.00",
		Items: []domain.OrderItemPayload{
			{ProductID: productID, Quantity: 4, Price: "250.00"},
		},
	}

	if err := repo.Reserve(ctx, createEv); err != nil {
		t.Fatalf("expected Reserve to succeed, got %v", err)
	}

	if err := repo.Reserve(ctx, createEv); !errors.Is(err, repository.ErrAlreadyProcessed) {
		t.Fatalf("expected ErrAlreadyProcessed on duplicate Reserve, got %v", err)
	}

	var available, reserved int
	_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", productID).Scan(&available, &reserved)
	if available != 16 || reserved != 4 {
		t.Fatalf("expected available=16, reserved=4 after Reserve, got %d/%d", available, reserved)
	}

	cancelEv := domain.OrderCancelledEvent{
		EventID:   uuid.New(),
		EventType: "OrderCancelled",
		OrderID:   orderID,
		UserID:    createEv.UserID,
		Items:     createEv.Items,
	}
	if err := repo.Release(ctx, cancelEv); err != nil {
		t.Fatalf("expected Release to succeed, got %v", err)
	}

	if err := repo.Release(ctx, cancelEv); err != nil {
		t.Fatalf("expected duplicate Release to return nil, got %v", err)
	}

	_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", productID).Scan(&available, &reserved)
	if available != 20 || reserved != 0 {
		t.Fatalf("expected available=20, reserved=0 after Release, got %d/%d", available, reserved)
	}
}

func TestReserve_ThenConfirm_DeductsReservedStock(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	productID := uuid.New()
	seedProduct(t, pool, productID, "100.00", 15)
	orderID := uuid.New()

	createEv := domain.OrderCreatedEvent{
		EventID:     uuid.New(),
		EventType:   "OrderCreated",
		OrderID:     orderID,
		UserID:      uuid.New(),
		TotalAmount: "500.00",
		Items: []domain.OrderItemPayload{
			{ProductID: productID, Quantity: 5, Price: "100.00"},
		},
	}

	if err := repo.Reserve(ctx, createEv); err != nil {
		t.Fatalf("expected Reserve to succeed, got %v", err)
	}

	completeEv := domain.OrderCompletedEvent{
		EventID:   uuid.New(),
		EventType: "OrderCompleted",
		OrderID:   orderID,
		UserID:    createEv.UserID,
		Items:     createEv.Items,
	}

	if err := repo.Confirm(ctx, completeEv); err != nil {
		t.Fatalf("expected Confirm to succeed, got %v", err)
	}

	if err := repo.Confirm(ctx, completeEv); err != nil {
		t.Fatalf("expected duplicate Confirm to return nil, got %v", err)
	}

	var available, reserved int
	_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", productID).Scan(&available, &reserved)
	if available != 10 || reserved != 0 {
		t.Fatalf("expected available=10, reserved=0 after Confirm, got %d/%d", available, reserved)
	}
}

func TestReserve_PriceMismatchAndInvalidPrices(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	productID := uuid.New()
	seedProduct(t, pool, productID, "100.00", 10)

	invalidPrices := []string{"99.99", "NaN", "+Inf", "-Inf", "1e2", "0x1p1", "0.00", "-100.00", "100.123"}
	for _, badPrice := range invalidPrices {
		ev := domain.OrderCreatedEvent{
			EventID:     uuid.New(),
			EventType:   "OrderCreated",
			OrderID:     uuid.New(),
			UserID:      uuid.New(),
			TotalAmount: badPrice,
			Items: []domain.OrderItemPayload{
				{ProductID: productID, Quantity: 1, Price: badPrice},
			},
		}
		err := repo.Reserve(ctx, ev)
		if !errors.Is(err, repository.ErrPriceMismatch) {
			t.Fatalf("expected ErrPriceMismatch for price %q, got %v", badPrice, err)
		}
	}

	unknownEv := domain.OrderCreatedEvent{
		EventID:     uuid.New(),
		EventType:   "OrderCreated",
		OrderID:     uuid.New(),
		UserID:      uuid.New(),
		TotalAmount: "100.00",
		Items: []domain.OrderItemPayload{
			{ProductID: uuid.New(), Quantity: 1, Price: "100.00"},
		},
	}
	if err := repo.Reserve(ctx, unknownEv); !errors.Is(err, repository.ErrProductNotFound) {
		t.Fatalf("expected ErrProductNotFound, got %v", err)
	}

	// Verify CleanupProcessedOutboxEvents with batch limit
	_, _ = pool.Exec(ctx, "UPDATE outbox_events SET processed = TRUE, created_at = NOW() - INTERVAL '10 days'")
	deleted, err := repo.CleanupProcessedOutboxEvents(ctx, 7*24*time.Hour, 100)
	if err != nil || deleted == 0 {
		t.Fatalf("expected CleanupProcessedOutboxEvents to delete old processed rows, got deleted=%d, err=%v", deleted, err)
	}
}
