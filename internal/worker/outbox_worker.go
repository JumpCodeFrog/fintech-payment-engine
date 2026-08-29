package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/JumpCodeFrog/fintech-payment-engine/pkg/kafka"
)

type WorkerConfig struct {
	BatchSize    int
	PollInterval time.Duration
	MaxRetries   int
	Topic        string
}

type OutboxWorker struct {
	outboxRepo domain.OutboxRepository
	transactor domain.Transactor
	producer   kafka.Producer
	logger     *slog.Logger
	config     WorkerConfig
}

func NewOutboxWorker(
	outboxRepo domain.OutboxRepository,
	transactor domain.Transactor,
	producer kafka.Producer,
	logger *slog.Logger,
	config WorkerConfig,
) *OutboxWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboxWorker{
		outboxRepo: outboxRepo,
		transactor: transactor,
		producer:   producer,
		logger:     logger,
		config:     config,
	}
}

func (w *OutboxWorker) Start(ctx context.Context) error {
	if err := w.validateConfig(); err != nil {
		return err
	}

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.processBatch(context.WithoutCancel(ctx)); err != nil {
				w.logger.ErrorContext(ctx, "failed to process outbox batch", "error", err)
			}
			if ctx.Err() != nil {
				return nil
			}
		}
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context) error {
	return w.transactor.WithinTransaction(ctx, func(txCtx context.Context) error {
		events, err := w.outboxRepo.GetPending(txCtx, w.config.BatchSize)
		if err != nil {
			return fmt.Errorf("get pending outbox events: %w", err)
		}

		for _, event := range events {
			publishErr := w.producer.Publish(
				txCtx,
				w.config.Topic,
				[]byte(event.AggregateID),
				event.Payload,
			)
			if publishErr != nil {
				event.RetryCount++
				if event.RetryCount >= w.config.MaxRetries {
					event.Status = domain.OutboxStatusFailed
				}
				w.logger.WarnContext(txCtx, "failed to publish outbox event",
					"event_id", event.ID,
					"retry_count", event.RetryCount,
					"error", publishErr,
				)
			} else {
				now := time.Now().UTC()
				event.Status = domain.OutboxStatusPublished
				event.PublishedAt = &now
			}

			if err := w.outboxRepo.Update(txCtx, event); err != nil {
				return fmt.Errorf("update outbox event %s: %w", event.ID, err)
			}
		}

		return nil
	})
}

func (w *OutboxWorker) validateConfig() error {
	if w.config.BatchSize <= 0 {
		return fmt.Errorf("outbox worker batch size must be positive")
	}
	if w.config.PollInterval <= 0 {
		return fmt.Errorf("outbox worker poll interval must be positive")
	}
	if w.config.MaxRetries <= 0 {
		return fmt.Errorf("outbox worker max retries must be positive")
	}
	if w.config.Topic == "" {
		return fmt.Errorf("outbox worker topic must not be empty")
	}
	return nil
}
