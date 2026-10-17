package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MaxOutboxAttempts   = 5
	outboxBatchSize     = 50
	publishConfirmLimit = 5 * time.Second
)

type DeferredConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

type RelayChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
}

type ConfirmPublishFunc func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (DeferredConfirmation, error)

type OutboxRelay struct {
	pool       *pgxpool.Pool
	ch         RelayChannel
	publishFn  ConfirmPublishFunc
	returns    <-chan amqp.Return
	maxRetries int
}

func NewOutboxRelay(pool *pgxpool.Pool, ch RelayChannel) *OutboxRelay {
	relay := &OutboxRelay{
		pool:       pool,
		ch:         ch,
		maxRetries: MaxOutboxAttempts,
	}
	if ch != nil {
		relay.publishFn = func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (DeferredConfirmation, error) {
			return ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
		}
	}
	return relay
}

func NewOutboxRelayWithPublisher(pool *pgxpool.Pool, publishFn ConfirmPublishFunc, returns <-chan amqp.Return) *OutboxRelay {
	return &OutboxRelay{
		pool:       pool,
		publishFn:  publishFn,
		returns:    returns,
		maxRetries: MaxOutboxAttempts,
	}
}

func (r *OutboxRelay) Run(ctx context.Context) error {
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

	if err := r.ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	r.returns = r.ch.NotifyReturn(make(chan amqp.Return, 100))
	notifyClose := r.ch.NotifyClose(make(chan *amqp.Error, 1))

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr, ok := <-notifyClose:
			if ok && amqpErr != nil {
				return fmt.Errorf("outbox relay amqp channel closed: %w", amqpErr)
			}
			return fmt.Errorf("outbox relay amqp channel closed")
		case <-cleanupTicker.C:
			if _, err := r.pool.Exec(ctx, "DELETE FROM outbox_events WHERE processed = TRUE AND created_at < NOW() - INTERVAL '7 days'"); err != nil {
				slog.Warn("Failed to cleanup old outbox events", "error", err)
			}
		case <-ticker.C:
			if err := r.ProcessBatch(ctx); err != nil {
				slog.Error("Failed to process outbox batch", "error", err)
			}
		}
	}
}

func (r *OutboxRelay) drainReturns() {
	if r.returns == nil {
		return
	}
	for {
		select {
		case <-r.returns:
		default:
			return
		}
	}
}

func (r *OutboxRelay) ProcessBatch(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, topic, payload, attempts
		FROM outbox_events
		WHERE processed = FALSE
		  AND attempts < $1
		  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, r.maxRetries, outboxBatchSize)
	if err != nil {
		return fmt.Errorf("failed to query outbox events: %w", err)
	}
	defer rows.Close()

	type eventRecord struct {
		id       uuid.UUID
		topic    string
		payload  []byte
		attempts int
	}

	var events []eventRecord
	for rows.Next() {
		var e eventRecord
		if err := rows.Scan(&e.id, &e.topic, &e.payload, &e.attempts); err != nil {
			return fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}
	rows.Close()

	if len(events) == 0 {
		return nil
	}

	r.drainReturns()

	var batchErr error
	for _, e := range events {
		pubErr := r.publishSingle(ctx, e.id, e.topic, e.payload)
		if pubErr != nil {
			nextAttempt := e.attempts + 1
			backoffSeconds := 1 << min(nextAttempt, 6)
			_, dbErr := tx.Exec(ctx, `
				UPDATE outbox_events
				SET attempts = $2,
				    last_error = $3,
				    next_retry_at = NOW() + ($4 * INTERVAL '1 second')
				WHERE id = $1
			`, e.id, nextAttempt, pubErr.Error(), backoffSeconds)
			if dbErr != nil {
				return fmt.Errorf("failed to record failure for outbox event %s: %w", e.id, dbErr)
			}

			if nextAttempt >= r.maxRetries {
				slog.Error("Outbox event parked after reaching max attempts", "event_id", e.id, "topic", e.topic, "attempts", nextAttempt, "error", pubErr)
			} else {
				slog.Warn("Outbox event publish failed, scheduled for retry", "event_id", e.id, "topic", e.topic, "attempt", nextAttempt, "error", pubErr)
			}

			batchErr = errors.Join(batchErr, pubErr)
			continue
		}

		_, err = tx.Exec(ctx, `
			UPDATE outbox_events
			SET processed = TRUE,
			    last_error = NULL
			WHERE id = $1
		`, e.id)
		if err != nil {
			return fmt.Errorf("failed to mark outbox event %s as processed: %w", e.id, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit outbox batch state: %w", err)
	}

	return batchErr
}

func (r *OutboxRelay) publishSingle(ctx context.Context, id uuid.UUID, topic string, payload []byte) error {
	r.drainReturns()

	dc, err := r.publishFn(
		ctx,
		"logpose_events",
		topic,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    id.String(),
			Body:         payload,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish outbox event %s: %w", id, err)
	}

	cctx, cancel := context.WithTimeout(ctx, publishConfirmLimit)
	ok, err := dc.WaitContext(cctx)
	cancel()
	if err != nil {
		return fmt.Errorf("publish confirm error for %s: %w", id, err)
	}
	if !ok {
		return fmt.Errorf("publish nacked by broker for %s", id)
	}

	if r.returns != nil {
		select {
		case ret := <-r.returns:
			return fmt.Errorf("unroutable event %s (topic %s): %s", id, topic, ret.ReplyText)
		default:
		}
	}

	return nil
}
