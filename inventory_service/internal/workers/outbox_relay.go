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
	MaxOutboxAttempts   = 20
	MaxBackoffSeconds   = 300
	outboxBatchSize     = 50
	claimLeaseSeconds   = 30
	publishConfirmLimit = 5 * time.Second
	cleanupBatchLimit   = 1000
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

func (r *OutboxRelay) SetMaxRetries(maxRetries int) {
	if maxRetries > 0 {
		r.maxRetries = maxRetries
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
			if _, err := r.CleanupProcessedEvents(ctx, 7*24*time.Hour, cleanupBatchLimit); err != nil {
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

type eventRecord struct {
	id       uuid.UUID
	topic    string
	payload  []byte
	attempts int
}

// claimBatch atomically claims up to outboxBatchSize events with a short lease on next_retry_at
// and immediately commits the transaction so no DB locks are held during network I/O.
func (r *OutboxRelay) claimBatch(ctx context.Context) ([]eventRecord, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin claim tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE outbox_events
		SET next_retry_at = NOW() + ($3 * INTERVAL '1 second')
		WHERE id IN (
			SELECT id
			FROM outbox_events
			WHERE processed = FALSE
			  AND attempts < $1
			  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
			ORDER BY created_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, topic, payload, attempts
	`, r.maxRetries, outboxBatchSize, claimLeaseSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox events: %w", err)
	}
	defer rows.Close()

	var events []eventRecord
	for rows.Next() {
		var e eventRecord
		if err := rows.Scan(&e.id, &e.topic, &e.payload, &e.attempts); err != nil {
			return nil, fmt.Errorf("failed to scan claimed outbox event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claimed rows iteration error: %w", err)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit claim tx: %w", err)
	}

	return events, nil
}

func (r *OutboxRelay) ProcessBatch(ctx context.Context) error {
	events, err := r.claimBatch(ctx)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	r.drainReturns()

	var batchErr error
	for _, e := range events {
		pubErr := r.publishSingle(ctx, e.id, e.topic, e.payload)
		if pubErr != nil {
			nextAttempt := e.attempts + 1
			backoffSeconds := min(1<<min(nextAttempt, 9), MaxBackoffSeconds)
			_, dbErr := r.pool.Exec(ctx, `
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
				slog.Warn("Outbox event publish failed, scheduled for retry", "event_id", e.id, "topic", e.topic, "attempt", nextAttempt, "backoff_s", backoffSeconds, "error", pubErr)
			}

			batchErr = errors.Join(batchErr, pubErr)
			continue
		}

		_, err = r.pool.Exec(ctx, `
			UPDATE outbox_events
			SET processed = TRUE,
			    last_error = NULL,
			    next_retry_at = NULL
			WHERE id = $1
		`, e.id)
		if err != nil {
			return fmt.Errorf("failed to mark outbox event %s as processed: %w", e.id, err)
		}
	}

	return batchErr
}

// RequeueParkedEvents resets attempts and next_retry_at for parked events so they can be retried after recovery.
func (r *OutboxRelay) RequeueParkedEvents(ctx context.Context) (int64, error) {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = 0,
		    last_error = NULL,
		    next_retry_at = NULL
		WHERE processed = FALSE AND attempts >= $1
	`, r.maxRetries)
	if err != nil {
		return 0, fmt.Errorf("failed to requeue parked outbox events: %w", err)
	}
	return cmdTag.RowsAffected(), nil
}

// CleanupProcessedEvents deletes processed outbox events older than retention in bounded batches.
func (r *OutboxRelay) CleanupProcessedEvents(ctx context.Context, retention time.Duration, batchLimit int) (int64, error) {
	if batchLimit <= 0 {
		batchLimit = cleanupBatchLimit
	}
	cutoff := time.Now().UTC().Add(-retention)
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
