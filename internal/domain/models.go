package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// TransactionStatus describes the lifecycle state of a transaction.
type TransactionStatus string

const (
	TransactionStatusPending   TransactionStatus = "PENDING"
	TransactionStatusCompleted TransactionStatus = "COMPLETED"
	TransactionStatusFailed    TransactionStatus = "FAILED"
	TransactionStatusCancelled TransactionStatus = "CANCELLED"
)

// Account is the domain representation of a user's monetary account.
type Account struct {
	ID            uuid.UUID
	UserID        uuid.UUID
	Currency      string
	Balance       decimal.Decimal
	FrozenBalance decimal.Decimal
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Transaction represents a money transfer recorded in the ledger.
type Transaction struct {
	ID              uuid.UUID
	IdempotencyKey  string
	SourceAccountID *uuid.UUID
	TargetAccountID *uuid.UUID
	Amount          decimal.Decimal
	Currency        string
	Status          TransactionStatus
	ErrorReason     string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
