package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Postgres PostgresConfig
	Kafka    KafkaConfig
	GRPC     GRPCConfig
}

type PostgresConfig struct {
	URL string
}

type KafkaConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	PollInterval time.Duration
	MaxRetries   int
}

type GRPCConfig struct {
	Port string
}

func LoadConfig() (*Config, error) {
	batchSize, err := envInt("OUTBOX_BATCH_SIZE", 100)
	if err != nil {
		return nil, err
	}
	pollInterval, err := envDuration("OUTBOX_POLL_INTERVAL", time.Second)
	if err != nil {
		return nil, err
	}
	maxRetries, err := envInt("OUTBOX_MAX_RETRIES", 5)
	if err != nil {
		return nil, err
	}

	brokers := strings.Split(envString("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		if brokers[i] == "" {
			return nil, fmt.Errorf("KAFKA_BROKERS contains an empty broker address")
		}
	}

	return &Config{
		Postgres: PostgresConfig{
			URL: envString("POSTGRES_URL", "postgres://postgres:password@localhost:5432/fintech_db?sslmode=disable"),
		},
		Kafka: KafkaConfig{
			Brokers:      brokers,
			Topic:        envString("KAFKA_TOPIC", "payment-events"),
			BatchSize:    batchSize,
			PollInterval: pollInterval,
			MaxRetries:   maxRetries,
		},
		GRPC: GRPCConfig{
			Port: envString("GRPC_PORT", ":50051"),
		},
	}, nil
}

func envString(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) (int, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}
