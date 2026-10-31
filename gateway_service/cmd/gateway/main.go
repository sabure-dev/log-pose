package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"gateway_service/internal/auth"
	"gateway_service/internal/config"
	"gateway_service/internal/handler"
	"gateway_service/internal/inventory"
	"gateway_service/internal/proxy"
	"gateway_service/internal/telemetry"
)

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(jsonHandler))

	if err := run(); err != nil {
		slog.Error("Gateway service terminated with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	slog.Info("Starting Gateway Service",
		"port", cfg.Port,
		"order_service_url", cfg.OrderServiceURL,
		"inventory_grpc", fmt.Sprintf("%s:%s", cfg.InventoryGRPCHost, cfg.InventoryGRPCPort),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracer, err := telemetry.InitTracer(ctx, "gateway-service", cfg.OTLPEndpoint)
	if err != nil {
		slog.Warn("Failed to initialize OpenTelemetry", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}

	var userStore auth.UserStore
	if cfg.DatabaseURL != "" {
		slog.Info("Connecting to PostgreSQL for Gateway user storage...")
		poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("failed to parse database URL: %w", err)
		}
		poolConfig.MaxConns = 10
		poolConfig.MinConns = 2
		poolConfig.MaxConnLifetime = 1 * time.Hour
		poolConfig.MaxConnIdleTime = 30 * time.Minute

		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("failed to connect to postgres: %w", err)
		}
		defer pool.Close()

		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("failed to ping postgres: %w", err)
		}

		pgStore, err := auth.NewPostgresUserStore(ctx, pool)
		if err != nil {
			return fmt.Errorf("failed to initialize postgres user store: %w", err)
		}
		userStore = pgStore
		slog.Info("PostgreSQL user store initialized successfully")
	} else {
		slog.Info("DATABASE_URL not configured; falling back to in-memory user store")
		userStore = auth.NewMemoryUserStore()
	}

	tokenManager := auth.NewTokenManager(cfg.JWTSecret, 24*time.Hour)
	authHandler := auth.NewHandler(userStore, tokenManager)

	invTarget := fmt.Sprintf("%s:%s", cfg.InventoryGRPCHost, cfg.InventoryGRPCPort)
	invClient, err := inventory.NewClient(invTarget)
	if err != nil {
		return fmt.Errorf("failed to create inventory gRPC client: %w", err)
	}
	defer invClient.Close()
	invHandler := inventory.NewHandler(invClient)

	ordersProxy, err := proxy.NewOrdersProxy(cfg.OrderServiceURL, cfg.InternalAPIToken)
	if err != nil {
		return fmt.Errorf("failed to create orders reverse proxy: %w", err)
	}

	appRouter := handler.NewRouter(&handler.RouterConfig{
		AuthHandler:      authHandler,
		InventoryHandler: invHandler,
		OrdersProxy:      ordersProxy,
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      appRouter,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		slog.Info("Gateway HTTP server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("Shutting down Gateway gracefully...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}
	case err := <-errChan:
		return fmt.Errorf("server error: %w", err)
	}

	slog.Info("Gateway Service stopped cleanly")
	return nil
}
