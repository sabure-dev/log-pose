package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"inventory_service/internal/config"
	"inventory_service/internal/consumer"
	"inventory_service/internal/grpcserver"
	pb "inventory_service/internal/pb/inventoryv1"
	"inventory_service/internal/repository"
	"inventory_service/internal/telemetry"
	"inventory_service/internal/workers"
	"inventory_service/migrations"
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

	// Initialize OpenTelemetry tracer
	shutdownTracer, err := telemetry.InitTracer(ctx, "inventory-service", cfg.OTLPEndpoint)
	if err != nil {
		slog.Warn("Failed to initialize OpenTelemetry", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}
	slog.Info("Connected to PostgreSQL")

	if err := migrations.Apply(ctx, pool); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	slog.Info("Database migrations applied successfully")

	if cfg.SeedCatalog {
		if err := migrations.SeedCatalog(ctx, pool); err != nil {
			return fmt.Errorf("failed to seed catalog: %w", err)
		}
		slog.Info("Demo catalog seeded successfully")
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

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port %s: %w", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcserver.UnaryLoggingInterceptor()),
	)
	pb.RegisterInventoryServiceServer(grpcServer, grpcserver.NewServer(repo))
	reflection.Register(grpcServer)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return orderConsumer.Run(gCtx)
	})

	g.Go(func() error {
		return outboxRelay.Run(gCtx)
	})

	g.Go(func() error {
		slog.Info("gRPC server listening", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("grpc server error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		slog.Info("Stopping gRPC server gracefully...")
		grpcServer.GracefulStop()
		return nil
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("error during execution: %w", err)
	}

	slog.Info("Inventory Service exited cleanly")
	return nil
}
