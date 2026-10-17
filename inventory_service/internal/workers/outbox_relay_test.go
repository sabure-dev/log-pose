package workers_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/workers"
	"inventory_service/migrations"
)

const testDBAdvisoryLockID = 74920183

type stubConfirm struct {
	ok  bool
	err error
}

func (s stubConfirm) WaitContext(ctx context.Context) (bool, error) {
	return s.ok, s.err
}

func setupRelayTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping postgres integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}

	// Acquire a session-level advisory lock on a dedicated connection so parallel package runs don't conflict or deadlock.
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

	if err := migrations.Apply(ctx, pool, false); err != nil {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testDBAdvisoryLockID)
		lockConn.Release()
		pool.Close()
		t.Fatalf("failed to apply migrations: %v", err)
	}

	// Relay tests only mutate outbox_events; clean up without locking unrelated tables.
	if _, err := pool.Exec(ctx, "DELETE FROM outbox_events;"); err != nil {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testDBAdvisoryLockID)
		lockConn.Release()
		pool.Close()
		t.Fatalf("failed to clean outbox_events: %v", err)
	}

	t.Cleanup(func() {
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testDBAdvisoryLockID)
		lockConn.Release()
		pool.Close()
	})
	return pool
}

func TestOutboxRelay_PartialFailureDoesNotRollbackConfirmedEvents(t *testing.T) {
	pool := setupRelayTestDB(t)
	ctx := context.Background()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, payload, created_at)
		VALUES
			($1, 'inventory.reserved', '{"status":"RESERVED"}'::jsonb, NOW() - INTERVAL '3 seconds'),
			($2, 'inventory.failed',   '{"status":"FAILED"}'::jsonb,   NOW() - INTERVAL '2 seconds'),
			($3, 'inventory.reserved', '{"status":"RESERVED"}'::jsonb, NOW() - INTERVAL '1 second')
	`, id1, id2, id3)
	if err != nil {
		t.Fatalf("failed to insert outbox events: %v", err)
	}

	returnsCh := make(chan amqp.Return, 10)
	var publishedIDs []string

	publishFn := func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (workers.DeferredConfirmation, error) {
		publishedIDs = append(publishedIDs, msg.MessageId)
		if msg.MessageId == id2.String() {
			// Simulate broker Nack (!ok && err == nil)
			return stubConfirm{ok: false, err: nil}, nil
		}
		return stubConfirm{ok: true, err: nil}, nil
	}

	relay := workers.NewOutboxRelayWithPublisher(pool, publishFn, returnsCh)
	batchErr := relay.ProcessBatch(ctx)
	if batchErr == nil {
		t.Fatalf("expected non-nil batchErr when one event was nacked")
	}

	// Verify event 1 and event 3 ARE marked processed=true, while event 2 has processed=false, attempts=1, and last_error set!
	var p1, p2, p3 bool
	var attempts2 int
	var lastErr2 *string
	_ = pool.QueryRow(ctx, "SELECT processed FROM outbox_events WHERE id = $1", id1).Scan(&p1)
	_ = pool.QueryRow(ctx, "SELECT processed, attempts, last_error FROM outbox_events WHERE id = $1", id2).Scan(&p2, &attempts2, &lastErr2)
	_ = pool.QueryRow(ctx, "SELECT processed FROM outbox_events WHERE id = $1", id3).Scan(&p3)

	if !p1 || !p3 {
		t.Fatalf("expected events 1 and 3 to remain committed as processed=true, got p1=%v, p3=%v", p1, p3)
	}
	if p2 || attempts2 != 1 || lastErr2 == nil {
		t.Fatalf("expected event 2 to have processed=false, attempts=1, last_error!=nil; got processed=%v, attempts=%d, err=%v", p2, attempts2, lastErr2)
	}
}

func TestOutboxRelay_ParksEventAfterMaxAttempts(t *testing.T) {
	pool := setupRelayTestDB(t)
	ctx := context.Background()

	poisonID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, payload, attempts)
		VALUES ($1, 'inventory.failed', '{"status":"FAILED"}'::jsonb, $2)
	`, poisonID, workers.MaxOutboxAttempts-1)
	if err != nil {
		t.Fatalf("failed to seed poison event: %v", err)
	}

	publishFn := func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (workers.DeferredConfirmation, error) {
		return nil, errors.New("broker rejected message")
	}

	relay := workers.NewOutboxRelayWithPublisher(pool, publishFn, nil)
	_ = relay.ProcessBatch(ctx)

	var attempts int
	_ = pool.QueryRow(ctx, "SELECT attempts FROM outbox_events WHERE id = $1", poisonID).Scan(&attempts)
	if attempts != workers.MaxOutboxAttempts {
		t.Fatalf("expected attempts=%d, got %d", workers.MaxOutboxAttempts, attempts)
	}

	// Next ProcessBatch should skip the parked event completely
	called := false
	relay2 := workers.NewOutboxRelayWithPublisher(pool, func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (workers.DeferredConfirmation, error) {
		called = true
		return stubConfirm{ok: true}, nil
	}, nil)

	if err := relay2.ProcessBatch(ctx); err != nil {
		t.Fatalf("expected nil error when poison event is parked, got %v", err)
	}
	if called {
		t.Fatalf("expected parked event not to be republished")
	}
}
