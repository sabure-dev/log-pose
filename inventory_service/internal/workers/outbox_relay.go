package workers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OutboxRelay struct {
	pool *pgxpool.Pool
	ch   *amqp.Channel
}

func NewOutboxRelay(pool *pgxpool.Pool, ch *amqp.Channel) *OutboxRelay {
	return &OutboxRelay{pool: pool, ch: ch}
}

func (r *OutboxRelay) Start(ctx context.Context) error {
	err := r.ch.ExchangeDeclare(
		"logpose_events",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.processBatch(ctx); err != nil {
				slog.Error("Failed to process outbox batch", "error", err)
			}
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, topic, payload
		FROM outbox_events
		WHERE processed = FALSE
		ORDER BY created_at ASC
		LIMIT 50
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return fmt.Errorf("failed to query outbox events: %w", err)
	}
	defer rows.Close()

	type eventRecord struct {
		id      uuid.UUID
		topic   string
		payload []byte
	}

	var events []eventRecord
	for rows.Next() {
		var e eventRecord
		if err := rows.Scan(&e.id, &e.topic, &e.payload); err != nil {
			return fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}
	rows.Close()

	if len(events) == 0 {
		return nil
	}

	for _, e := range events {
		err := r.ch.PublishWithContext(
			ctx,
			"logpose_events",
			e.topic,
			true,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    e.id.String(),
				Body:         e.payload,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to publish outbox event %s: %w", e.id, err)
		}

		_, err = tx.Exec(ctx, `
			UPDATE outbox_events
			SET processed = TRUE
			WHERE id = $1
		`, e.id)
		if err != nil {
			return fmt.Errorf("failed to mark outbox event %s as processed: %w", e.id, err)
		}
	}

	return tx.Commit(ctx)
}
