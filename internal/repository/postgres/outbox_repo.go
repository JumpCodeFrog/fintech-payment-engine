package postgres

import (
	"context"
	"fmt"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) Create(ctx context.Context, event *domain.OutboxEvent) error {
	_, err := dbtx(ctx, r.pool).Exec(ctx, `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload,
			status, retry_count, created_at, published_at
		)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)`,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		string(event.Payload),
		event.Status,
		event.RetryCount,
		event.CreatedAt,
		event.PublishedAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (r *OutboxRepository) GetPending(ctx context.Context, limit int) ([]*domain.OutboxEvent, error) {
	rows, err := dbtx(ctx, r.pool).Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload::text,
		       status, retry_count, created_at, published_at
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]*domain.OutboxEvent, 0)
	for rows.Next() {
		var event domain.OutboxEvent
		var payload string
		var publishedAt pgtype.Timestamptz
		if err := rows.Scan(
			&event.ID,
			&event.AggregateType,
			&event.AggregateID,
			&event.EventType,
			&payload,
			&event.Status,
			&event.RetryCount,
			&event.CreatedAt,
			&publishedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending outbox event: %w", err)
		}
		if publishedAt.Valid {
			event.PublishedAt = &publishedAt.Time
		}
		event.Payload = []byte(payload)
		events = append(events, &event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox events: %w", err)
	}

	return events, nil
}

func (r *OutboxRepository) Update(ctx context.Context, event *domain.OutboxEvent) error {
	result, err := dbtx(ctx, r.pool).Exec(ctx, `
		UPDATE outbox_events
		SET status = $2, retry_count = $3, published_at = $4
		WHERE id = $1`,
		event.ID,
		event.Status,
		event.RetryCount,
		event.PublishedAt,
	)
	if err != nil {
		return fmt.Errorf("update outbox event: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("update outbox event %s: not found", event.ID)
	}
	return nil
}

var _ domain.OutboxRepository = (*OutboxRepository)(nil)
