package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"inventory_service/internal/domain"
)

var (
	ErrAlreadyProcessed = errors.New("event already processed")
	ErrOutOfStock       = errors.New("insufficient stock")
)

type InventoryRepository struct {
	pool *pgxpool.Pool
}

func NewInventoryRepository(pool *pgxpool.Pool) *InventoryRepository {
	return &InventoryRepository{pool: pool}
}

func (r *InventoryRepository) ReserveItems(ctx context.Context, eventID uuid.UUID, items []domain.OrderItemPayload) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM inbox_events WHERE event_id = $1)", eventID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("inbox check failed: %w", err)
	}
	if exists {
		return ErrAlreadyProcessed
	}

	sortedItems := make([]domain.OrderItemPayload, len(items))
	copy(sortedItems, items)
	sort.Slice(sortedItems, func(i, j int) bool {
		return sortedItems[i].ProductID.String() < sortedItems[j].ProductID.String()
	})

	for _, item := range sortedItems {
		cmdTag, err := tx.Exec(ctx, `
			UPDATE inventory_stocks 
			SET stock = stock - $1, 
			    reserved = reserved + $1,
			    updated_at = NOW()
			WHERE product_id = $2 AND stock >= $1
		`, item.Quantity, item.ProductID)

		if err != nil {
			return fmt.Errorf("stock update error for %s: %w", item.ProductID, err)
		}
		if cmdTag.RowsAffected() == 0 {
			return fmt.Errorf("%w for product %s", ErrOutOfStock, item.ProductID)
		}
	}

	_, err = tx.Exec(ctx, "INSERT INTO inbox_events (event_id, event_type) VALUES ($1, $2)", eventID, "OrderCreated")
	if err != nil {
		return fmt.Errorf("failed to record inbox event: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *InventoryRepository) ReleaseItems(ctx context.Context, eventID uuid.UUID, items []domain.OrderItemPayload) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM inbox_events WHERE event_id = $1)", eventID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("inbox check failed: %w", err)
	}
	if exists {
		return ErrAlreadyProcessed
	}

	sortedItems := make([]domain.OrderItemPayload, len(items))
	copy(sortedItems, items)
	sort.Slice(sortedItems, func(i, j int) bool {
		return sortedItems[i].ProductID.String() < sortedItems[j].ProductID.String()
	})

	for _, item := range sortedItems {
		_, err := tx.Exec(ctx, `
			UPDATE inventory_stocks 
			SET stock = stock + $1, 
			    reserved = GREATEST(0, reserved - $1),
			    updated_at = NOW()
			WHERE product_id = $2
		`, item.Quantity, item.ProductID)

		if err != nil {
			return fmt.Errorf("stock release error for %s: %w", item.ProductID, err)
		}
	}

	_, err = tx.Exec(ctx, "INSERT INTO inbox_events (event_id, event_type) VALUES ($1, $2)", eventID, "OrderCancelled")
	if err != nil {
		return fmt.Errorf("failed to record inbox event: %w", err)
	}

	return tx.Commit(ctx)
}
