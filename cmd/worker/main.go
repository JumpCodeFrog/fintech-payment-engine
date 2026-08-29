package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	postgresrepo "github.com/JumpCodeFrog/fintech-payment-engine/internal/repository/postgres"
	"github.com/JumpCodeFrog/fintech-payment-engine/internal/worker"
	"github.com/JumpCodeFrog/fintech-payment-engine/pkg/config"
	"github.com/JumpCodeFrog/fintech-payment-engine/pkg/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("outbox worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.Postgres.URL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}

	producer := kafka.NewProducer(cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("failed to close Kafka producer", "error", err)
		}
	}()

	outboxRepo := postgresrepo.NewOutboxRepository(pool)
	transactor := postgresrepo.NewTransactor(pool)
	outboxWorker := worker.NewOutboxWorker(
		outboxRepo,
		transactor,
		producer,
		logger,
		worker.WorkerConfig{
			BatchSize:    cfg.Kafka.BatchSize,
			PollInterval: cfg.Kafka.PollInterval,
			MaxRetries:   cfg.Kafka.MaxRetries,
			Topic:        cfg.Kafka.Topic,
		},
	)

	logger.Info("outbox worker started", "topic", cfg.Kafka.Topic)
	if err := outboxWorker.Start(ctx); err != nil {
		return err
	}
	logger.Info("outbox worker stopped")
	return nil
}
