package repository_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL or DATABASE_URL not set, skipping postgres integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}

	migrationPath := filepath.Join("..", "..", "migrations", "000001_init_inventory.sql")
	sqlBytes, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("failed to read migration: %v", err)
	}

	if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
		t.Fatalf("failed to apply migration: %v", err)
	}

	_, err = pool.Exec(ctx, "TRUNCATE TABLE outbox_events, reservations, inventory_stocks, products CASCADE")
	if err != nil {
		t.Fatalf("failed to truncate tables: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})
	return pool
}

func seedProduct(t *testing.T, pool *pgxpool.Pool, productID uuid.UUID, price string, available int) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO products (id, name, price) VALUES ($1, 'Test Item', $2::numeric)
	`, productID, price)
	if err != nil {
		t.Fatalf("failed to seed product: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO inventory_stocks (product_id, available, reserved) VALUES ($1, $2, 0)
	`, productID, available)
	if err != nil {
		t.Fatalf("failed to seed stock: %v", err)
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

func TestReserveFailed_ThenCancel_DoesNotInflateStock(t *testing.T) {
	pool := setupTestDB(t)
	repo := repository.NewInventoryRepository(pool)
	ctx := context.Background()

	keyboardID := uuid.New()
	mouseID := uuid.New()
	seedProduct(t, pool, keyboardID, "1500.50", 100)
	seedProduct(t, pool, mouseID, "300.00", 50)

	orderID := uuid.New()
	createEv := domain.OrderCreatedEvent{
		EventID:     uuid.New(),
		EventType:   "OrderCreated",
		OrderID:     orderID,
		UserID:      uuid.New(),
		TotalAmount: "301500.50",
		Items: []domain.OrderItemPayload{
			{ProductID: keyboardID, Quantity: 1, Price: "1500.50"},
			{ProductID: mouseID, Quantity: 1000, Price: "300.00"},
		},
	}

	err := repo.Reserve(ctx, createEv)
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
	_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", keyboardID).Scan(&kbAvail, &kbRes)
	_ = pool.QueryRow(ctx, "SELECT available, reserved FROM inventory_stocks WHERE product_id = $1", mouseID).Scan(&mouseAvail, &mouseRes)

	if kbAvail != 100 || kbRes != 0 {
		t.Fatalf("expected keyboard 100/0, got %d/%d", kbAvail, kbRes)
	}
	if mouseAvail != 50 || mouseRes != 0 {
		t.Fatalf("expected mouse 50/0, got %d/%d", mouseAvail, mouseRes)
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
