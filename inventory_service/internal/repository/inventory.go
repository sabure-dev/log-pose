package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"inventory_service/internal/domain"
)

var (
	ErrAlreadyProcessed     = errors.New("event already processed")
	ErrOutOfStock           = errors.New("insufficient stock")
	ErrInvalidQuantity      = errors.New("quantity must be greater than zero")
	ErrPriceMismatch        = errors.New("item price does not match catalog")
	ErrCorruptedReservation = errors.New("corrupted reservation state")
)

type Reserver interface {
	Reserve(ctx context.Context, event domain.OrderCreatedEvent) error
	Release(ctx context.Context, event domain.OrderCancelledEvent) error
}

type InventoryRepository struct {
	pool *pgxpool.Pool
}

func NewInventoryRepository(pool *pgxpool.Pool) *InventoryRepository {
	return &InventoryRepository{pool: pool}
}

func (r *InventoryRepository) Reserve(ctx context.Context, event domain.OrderCreatedEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	itemsJSON, err := json.Marshal(event.Items)
	if err != nil {
		return fmt.Errorf("failed to marshal items: %w", err)
	}

	cmdTag, err := tx.Exec(ctx, `
		INSERT INTO reservations (order_id, status, items)
		VALUES ($1, 'RESERVED', $2)
		ON CONFLICT (order_id) DO NOTHING
	`, event.OrderID, itemsJSON)
	if err != nil {
		return fmt.Errorf("failed to insert reservation: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrAlreadyProcessed
	}

	if len(event.Items) == 0 {
		return r.failReservation(ctx, tx, event, "empty items list", ErrInvalidQuantity)
	}

	for _, item := range event.Items {
		if item.Quantity <= 0 {
			return r.failReservation(ctx, tx, event, "quantity must be greater than zero", ErrInvalidQuantity)
		}
		if item.Price == "" {
			return r.failReservation(ctx, tx, event, "missing item price", ErrPriceMismatch)
		}
		if _, err := strconv.ParseFloat(item.Price, 64); err != nil {
			return r.failReservation(ctx, tx, event, "invalid item price format", ErrPriceMismatch)
		}

		var priceMatches bool
		err := tx.QueryRow(
			ctx,
			"SELECT price = $1::numeric FROM products WHERE id = $2",
			item.Price,
			item.ProductID,
		).Scan(&priceMatches)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return r.failReservation(ctx, tx, event, "product not found in catalog", ErrOutOfStock)
			}
			return fmt.Errorf("failed to check price for %s: %w", item.ProductID, err)
		}
		if !priceMatches {
			return r.failReservation(ctx, tx, event, fmt.Sprintf("price mismatch for %s", item.ProductID), ErrPriceMismatch)
		}
	}

	sortedItems := make([]domain.OrderItemPayload, len(event.Items))
	copy(sortedItems, event.Items)
	slices.SortFunc(sortedItems, func(a, b domain.OrderItemPayload) int {
		return bytes.Compare(a.ProductID[:], b.ProductID[:])
	})

	for _, item := range sortedItems {
		cmdTag, err := tx.Exec(ctx, `
			UPDATE inventory_stocks
			SET available = available - $1,
			    reserved = reserved + $1,
			    updated_at = NOW()
			WHERE product_id = $2 AND available >= $1
		`, item.Quantity, item.ProductID)
		if err != nil {
			return fmt.Errorf("stock update error for %s: %w", item.ProductID, err)
		}
		if cmdTag.RowsAffected() == 0 {
			return r.failReservation(ctx, tx, event, "insufficient stock", ErrOutOfStock)
		}
	}

	reservedEvent := domain.InventoryReservedEvent{
		EventID:       uuid.New(),
		CorrelationID: event.CorrelationID,
		EventType:     "InventoryReservationResult",
		OrderID:       event.OrderID,
		Status:        "RESERVED",
	}
	payloadBytes, err := json.Marshal(reservedEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, payload)
		VALUES ($1, 'inventory.reserved', $2)
	`, reservedEvent.EventID, payloadBytes)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *InventoryRepository) failReservation(
	ctx context.Context,
	tx pgx.Tx,
	event domain.OrderCreatedEvent,
	reason string,
	domainErr error,
) error {
	_, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status = 'FAILED', updated_at = NOW()
		WHERE order_id = $1
	`, event.OrderID)
	if err != nil {
		return fmt.Errorf("failed to mark reservation as failed: %w", err)
	}

	failedEvent := domain.InventoryReservedEvent{
		EventID:       uuid.New(),
		CorrelationID: event.CorrelationID,
		EventType:     "InventoryReservationResult",
		OrderID:       event.OrderID,
		Status:        "FAILED",
		Reason:        reason,
	}
	payloadBytes, err := json.Marshal(failedEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal failed outbox event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, payload)
		VALUES ($1, 'inventory.failed', $2)
	`, failedEvent.EventID, payloadBytes)
	if err != nil {
		return fmt.Errorf("failed to insert failed outbox event: %w", err)
	}

	if commitErr := tx.Commit(ctx); commitErr != nil {
		return fmt.Errorf("failed to commit failed reservation: %w", commitErr)
	}

	return domainErr
}

func (r *InventoryRepository) Release(ctx context.Context, event domain.OrderCancelledEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var itemsJSON []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO reservations (order_id, status, items)
		VALUES ($1, 'RELEASED', '[]'::jsonb)
		ON CONFLICT (order_id) DO UPDATE
		    SET status = 'RELEASED', updated_at = NOW()
		    WHERE reservations.status = 'RESERVED'
		RETURNING items
	`, event.OrderID).Scan(&itemsJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to release reservation: %w", err)
	}

	var reservedItems []domain.OrderItemPayload
	if err := json.Unmarshal(itemsJSON, &reservedItems); err != nil {
		return fmt.Errorf("failed to unmarshal reserved items: %w", err)
	}

	slices.SortFunc(reservedItems, func(a, b domain.OrderItemPayload) int {
		return bytes.Compare(a.ProductID[:], b.ProductID[:])
	})

	for _, item := range reservedItems {
		cmdTag, err := tx.Exec(ctx, `
			UPDATE inventory_stocks
			SET available = available + $1,
			    reserved = reserved - $1,
			    updated_at = NOW()
			WHERE product_id = $2 AND reserved >= $1
		`, item.Quantity, item.ProductID)
		if err != nil {
			return fmt.Errorf("failed to restore stock for %s: %w", item.ProductID, err)
		}
		if cmdTag.RowsAffected() == 0 {
			return fmt.Errorf("%w: product %s reserved count underflow", ErrCorruptedReservation, item.ProductID)
		}
	}

	return tx.Commit(ctx)
}
