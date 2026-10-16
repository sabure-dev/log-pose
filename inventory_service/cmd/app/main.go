package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/config"
	"inventory_service/internal/consumer"
	"inventory_service/internal/repository"
)

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(jsonHandler))

	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("Database ping failed", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to PostgreSQL")

	initSchema(ctx, pool)

	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		slog.Error("RabbitMQ connection failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		slog.Error("RabbitMQ channel creation failed", "error", err)
		os.Exit(1)
	}
	defer ch.Close()
	slog.Info("Connected to RabbitMQ")

	repo := repository.NewInventoryRepository(pool)
	orderConsumer := consumer.NewOrderConsumer(ch, repo)
	if err := orderConsumer.Start(ctx); err != nil {
		slog.Error("Failed to start consumer", "error", err)
		os.Exit(1)
	}

	<-ctx.Done()
	slog.Info("Shutting down Inventory Service, waiting for active jobs to finish...")
	orderConsumer.Wait()
	slog.Info("Inventory Service exited cleanly")
}

func initSchema(ctx context.Context, pool *pgxpool.Pool) {
	schema := `
	CREATE TABLE IF NOT EXISTS products (
		id UUID PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		price NUMERIC(10, 2) NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS inventory_stocks (
		product_id UUID PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
		stock INT NOT NULL CHECK (stock >= 0),
		reserved INT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS inbox_events (
		event_id UUID PRIMARY KEY,
		event_type VARCHAR(100) NOT NULL,
		processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	INSERT INTO products (id, name, price) VALUES 
	('3fa85f64-5717-4562-b3fc-2c963f66afa6', 'Mechanical Keyboard', 1500.50),
	('123e4567-e89b-12d3-a456-426614174000', 'Ergonomic Mouse', 300.00)
	ON CONFLICT (id) DO NOTHING;

	INSERT INTO inventory_stocks (product_id, stock, reserved) VALUES 
	('3fa85f64-5717-4562-b3fc-2c963f66afa6', 100, 0),
	('123e4567-e89b-12d3-a456-426614174000', 50, 0)
	ON CONFLICT (product_id) DO NOTHING;
	`
	if _, err := pool.Exec(ctx, schema); err != nil {
		slog.Warn("Schema auto-init check error", "error", err)
	} else {
		slog.Info("Inventory schema initialized")
	}
}
