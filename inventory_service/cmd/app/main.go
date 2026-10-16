package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
	"inventory_service/internal/config"
	"inventory_service/internal/consumer"
	"inventory_service/internal/repository"
	"inventory_service/internal/workers"
)

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(jsonHandler))

	if err := run(); err != nil {
		slog.Error("Service terminated with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}
	slog.Info("Connected to PostgreSQL")

	if err := applyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("rabbitmq connection failed: %w", err)
	}
	defer conn.Close()

	consumerCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq consumer channel failed: %w", err)
	}
	defer consumerCh.Close()

	relayCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq relay channel failed: %w", err)
	}
	defer relayCh.Close()
	slog.Info("Connected to RabbitMQ")

	repo := repository.NewInventoryRepository(pool)
	orderConsumer := consumer.NewOrderConsumer(consumerCh, repo)
	outboxRelay := workers.NewOutboxRelay(pool, relayCh)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return orderConsumer.Run(gCtx)
	})

	g.Go(func() error {
		return outboxRelay.Run(gCtx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("error during execution: %w", err)
	}

	slog.Info("Inventory Service exited cleanly")
	return nil
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	migrationSQL, err := os.ReadFile("migrations/000001_init_inventory.sql")
	if err != nil {
		return fmt.Errorf("could not read migration file: %w", err)
	}

	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		return fmt.Errorf("migration execution error: %w", err)
	}

	slog.Info("Database migrations applied successfully")
	return nil
}
