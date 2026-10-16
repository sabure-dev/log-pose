package config

import (
	"os"
)

type Config struct {
	DatabaseURL string
	RabbitMQURL string
}

func Load() *Config {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://inventory_user:inventory_password@localhost:5433/inventory_db?sslmode=disable"
	}

	rmqURL := os.Getenv("RABBITMQ_URL")
	if rmqURL == "" {
		rmqURL = "amqp://guest:guest@localhost:5672/"
	}

	return &Config{
		DatabaseURL: dbURL,
		RabbitMQURL: rmqURL,
	}
}
