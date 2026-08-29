package domain

import (
	"time"

	"github.com/google/uuid"
)

// OutboxStatus describes the delivery state of an outbox event.
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

// OutboxEvent is a domain event waiting to be delivered to a message broker.
type OutboxEvent struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Status        OutboxStatus
	RetryCount    int
	CreatedAt     time.Time
	PublishedAt   *time.Time
}
