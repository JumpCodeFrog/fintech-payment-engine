package domain

import (
	"context"

	"github.com/google/uuid"
)

// AccountRepository defines persistence operations required for accounts.
type AccountRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Account, error)
	GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*Account, error)
	Update(ctx context.Context, account *Account) error
}

// TransactionRepository defines persistence operations required for transactions.
type TransactionRepository interface {
	Create(ctx context.Context, transaction *Transaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*Transaction, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Transaction, error)
	Update(ctx context.Context, transaction *Transaction) error
}

// OutboxRepository defines persistence operations required for event delivery.
type OutboxRepository interface {
	Create(ctx context.Context, event *OutboxEvent) error
	GetPending(ctx context.Context, limit int) ([]*OutboxEvent, error)
	Update(ctx context.Context, event *OutboxEvent) error
}

// Transactor executes a set of operations atomically.
type Transactor interface {
	WithinTransaction(ctx context.Context, fn func(context.Context) error) error
}
