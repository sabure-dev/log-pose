package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	DatabaseURL  string
	RabbitMQURL  string
	SeedCatalog  bool
	GRPCPort     string
	OTLPEndpoint string
}

func Load() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	rmqURL := os.Getenv("RABBITMQ_URL")
	if rmqURL == "" {
		return nil, fmt.Errorf("RABBITMQ_URL environment variable is required")
	}

	seedRaw := strings.ToLower(strings.TrimSpace(os.Getenv("SEED_CATALOG")))
	seedCatalog := seedRaw == "true" || seedRaw == "1" || seedRaw == "yes"

	grpcPort := strings.TrimSpace(os.Getenv("GRPC_PORT"))
	if grpcPort == "" {
		grpcPort = "50051"
	}

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	return &Config{
		DatabaseURL:  dbURL,
		RabbitMQURL:  rmqURL,
		SeedCatalog:  seedCatalog,
		GRPCPort:     grpcPort,
		OTLPEndpoint: otlpEndpoint,
	}, nil
}
