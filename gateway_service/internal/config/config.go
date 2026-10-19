package config

import (
	"os"
	"strings"
)

type Config struct {
	Port              string
	OrderServiceURL   string
	InventoryGRPCHost string
	InventoryGRPCPort string
	JWTSecret         string
	InternalAPIToken  string
	OTLPEndpoint      string
}

func Load() *Config {
	port := strings.TrimSpace(os.Getenv("GATEWAY_PORT"))
	if port == "" {
		port = strings.TrimSpace(os.Getenv("PORT"))
	}
	if port == "" {
		port = "8080"
	}

	orderURL := strings.TrimSpace(os.Getenv("ORDER_SERVICE_URL"))
	if orderURL == "" {
		orderURL = "http://order_service:8000"
	}

	invHost := strings.TrimSpace(os.Getenv("INVENTORY_GRPC_HOST"))
	if invHost == "" {
		invHost = "inventory_service"
	}

	invPort := strings.TrimSpace(os.Getenv("INVENTORY_GRPC_PORT"))
	if invPort == "" {
		invPort = "50051"
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "logpose-jwt-secret-key-change-in-production"
	}

	internalToken := os.Getenv("INTERNAL_API_TOKEN")
	if internalToken == "" {
		internalToken = "logpose-internal-secret"
	}

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	return &Config{
		Port:              port,
		OrderServiceURL:   orderURL,
		InventoryGRPCHost: invHost,
		InventoryGRPCPort: invPort,
		JWTSecret:         jwtSecret,
		InternalAPIToken:  internalToken,
		OTLPEndpoint:      otlpEndpoint,
	}
}
