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

	// Initialize OpenTelemetry tracer
	shutdownTracer, err := telemetry.InitTracer(ctx, "gateway-service", cfg.OTLPEndpoint)
	if err != nil {
		slog.Warn("Failed to initialize OpenTelemetry", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}

	// 1. Initialize Auth module
	userStore := auth.NewMemoryUserStore()
	tokenManager := auth.NewTokenManager(cfg.JWTSecret, 24*time.Hour)
	authHandler := auth.NewHandler(userStore, tokenManager)

	// 2. Initialize Inventory gRPC client
	invTarget := fmt.Sprintf("%s:%s", cfg.InventoryGRPCHost, cfg.InventoryGRPCPort)
	invClient, err := inventory.NewClient(invTarget)
	if err != nil {
		return fmt.Errorf("failed to create inventory gRPC client: %w", err)
	}
	defer invClient.Close()
	invHandler := inventory.NewHandler(invClient)

	// 3. Initialize Orders Reverse Proxy
	ordersProxy, err := proxy.NewOrdersProxy(cfg.OrderServiceURL, cfg.InternalAPIToken)
	if err != nil {
		return fmt.Errorf("failed to create orders reverse proxy: %w", err)
	}

	// 4. Build HTTP Router
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
