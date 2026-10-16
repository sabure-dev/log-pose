package config

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL string
	RabbitMQURL string
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

	return &Config{
		DatabaseURL: dbURL,
		RabbitMQURL: rmqURL,
	}, nil
}
