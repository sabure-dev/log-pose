package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"inventory_service/internal/domain"
)

var (
	ErrAlreadyProcessed     = errors.New("event already processed")
	ErrOutOfStock           = errors.New("insufficient stock")
	ErrProductNotFound      = errors.New("product not found in catalog")
	ErrInvalidQuantity      = errors.New("quantity must be greater than zero")
	ErrPriceMismatch        = errors.New("item price does not match catalog")
	ErrInvalidEvent         = errors.New("invalid order event payload")
	ErrCorruptedReservation = errors.New("corrupted reservation state")
)

var decimalPriceRegex = regexp.MustCompile(`^\d{1,8}(\.\d{1,2})?$`)

func isValidPositiveDecimalPrice(raw string) bool {
	if !decimalPriceRegex.MatchString(raw) {
		return false
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil || val <= 0 {
		return false
	}
	return true
}

type Reserver interface {
	Reserve(ctx context.Context, event domain.OrderCreatedEvent) error
	Release(ctx context.Context, event domain.OrderCancelledEvent) error
	Confirm(ctx context.Context, event domain.OrderCompletedEvent) error
}

type InventoryRepository struct {
	pool *pgxpool.Pool
}

func NewInventoryRepository(pool *pgxpool.Pool) *InventoryRepository {
	return &InventoryRepository{pool: pool}
}

func (r *InventoryRepository) UpsertProduct(
	ctx context.Context,
	productID uuid.UUID,
	name string,
	price string,
	available int,
) error {
	if productID == uuid.Nil || name == "" || !isValidPositiveDecimalPrice(price) || available < 0 {
		return ErrInvalidEvent
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO products (id, name, price)
		VALUES ($1, $2, $3::numeric)
		ON CONFLICT (id) DO UPDATE
		SET name = EXCLUDED.name,
		    price = EXCLUDED.price
	`, productID, name, price)
	if err != nil {
		return fmt.Errorf("failed to upsert product: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO inventory_stocks (product_id, available, reserved)
		VALUES ($1, $2, 0)
		ON CONFLICT (product_id) DO UPDATE
		SET available = EXCLUDED.available,
		    updated_at = NOW()
	`, productID, available)
	if err != nil {
		return fmt.Errorf("failed to upsert stock: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *InventoryRepository) Reserve(ctx context.Context, event domain.OrderCreatedEvent) error {
	if event.OrderID == uuid.Nil {
		return ErrInvalidEvent
	}

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

	seenProducts := make(map[uuid.UUID]struct{}, len(event.Items))
	for _, item := range event.Items {
		if item.ProductID == uuid.Nil {
			return r.failReservation(ctx, tx, event, "invalid product_id", ErrInvalidEvent)
		}
		if _, exists := seenProducts[item.ProductID]; exists {
			return r.failReservation(ctx, tx, event, "duplicate product_id in order items", ErrInvalidEvent)
		}
		seenProducts[item.ProductID] = struct{}{}

		if item.Quantity <= 0 {
			return r.failReservation(ctx, tx, event, "quantity must be greater than zero", ErrInvalidQuantity)
		}
		if item.Price == "" {
			return r.failReservation(ctx, tx, event, "missing item price", ErrPriceMismatch)
		}
		if !isValidPositiveDecimalPrice(item.Price) {
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
				return r.failReservation(ctx, tx, event, "product not found in catalog", ErrProductNotFound)
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

	// Wrap stock updates in a savepoint so partial deductions are rolled back if a subsequent item is out of stock.
	sp, err := tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin stock savepoint: %w", err)
	}

	for _, item := range sortedItems {
		cmdTag, err := sp.Exec(ctx, `
			UPDATE inventory_stocks
			SET available = available - $1,
			    reserved = reserved + $1,
			    updated_at = NOW()
			WHERE product_id = $2 AND available >= $1
		`, item.Quantity, item.ProductID)
		if err != nil {
			_ = sp.Rollback(ctx)
			return fmt.Errorf("stock update error for %s: %w", item.ProductID, err)
		}
		if cmdTag.RowsAffected() == 0 {
			if rbErr := sp.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("failed to rollback stock savepoint: %w", rbErr)
			}
			return r.failReservation(ctx, tx, event, "insufficient stock", ErrOutOfStock)
		}
	}

	if err := sp.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit stock savepoint: %w", err)
	}

	reservedEvent := domain.InventoryReservationResultEvent{
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

	failedEvent := domain.InventoryReservationResultEvent{
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
	if event.OrderID == uuid.Nil {
		return ErrInvalidEvent
	}

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

func (r *InventoryRepository) Confirm(ctx context.Context, event domain.OrderCompletedEvent) error {
	if event.OrderID == uuid.Nil {
		return ErrInvalidEvent
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var itemsJSON []byte
	err = tx.QueryRow(ctx, `
		UPDATE reservations
		SET status = 'CONFIRMED', updated_at = NOW()
		WHERE order_id = $1 AND status = 'RESERVED'
		RETURNING items
	`, event.OrderID).Scan(&itemsJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var existingStatus string
			statusErr := tx.QueryRow(
				ctx,
				"SELECT status FROM reservations WHERE order_id = $1",
				event.OrderID,
			).Scan(&existingStatus)
			if statusErr != nil {
				if errors.Is(statusErr, pgx.ErrNoRows) {
					return fmt.Errorf("%w: reservation for order %s not found", ErrCorruptedReservation, event.OrderID)
				}
				return fmt.Errorf("failed to check reservation status for %s: %w", event.OrderID, statusErr)
			}
			if existingStatus == "CONFIRMED" {
				return nil
			}
			return fmt.Errorf(
				"%w: cannot confirm reservation for order %s (status=%q)",
				ErrCorruptedReservation,
				event.OrderID,
				existingStatus,
			)
		}
		return fmt.Errorf("failed to confirm reservation: %w", err)
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
			SET reserved = reserved - $1,
			    updated_at = NOW()
			WHERE product_id = $2 AND reserved >= $1
		`, item.Quantity, item.ProductID)
		if err != nil {
			return fmt.Errorf("failed to deduct reserved stock for %s: %w", item.ProductID, err)
		}
		if cmdTag.RowsAffected() == 0 {
			return fmt.Errorf("%w: product %s reserved count underflow on confirm", ErrCorruptedReservation, item.ProductID)
		}
	}

	return tx.Commit(ctx)
}

func (r *InventoryRepository) CleanupProcessedOutboxEvents(
	ctx context.Context,
	olderThan time.Duration,
	batchLimit int,
) (int64, error) {
	if batchLimit <= 0 {
		batchLimit = 1000
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	cmdTag, err := r.pool.Exec(ctx, `
		DELETE FROM outbox_events
		WHERE id IN (
			SELECT id FROM outbox_events
			WHERE processed = TRUE AND created_at < $1
			ORDER BY created_at ASC
			LIMIT $2
		)
	`, cutoff, batchLimit)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup processed outbox events: %w", err)
	}
	return cmdTag.RowsAffected(), nil
}
