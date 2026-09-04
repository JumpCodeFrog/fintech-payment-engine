package domain

import "errors"

var (
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrAccountNotFound      = errors.New("account not found")
	ErrInvalidAmount        = errors.New("invalid amount")
	ErrCurrencyMismatch     = errors.New("currency mismatch")
	ErrIdempotencyConflict  = errors.New("idempotency conflict")
	ErrBalanceLimitExceeded = errors.New("account balance limit exceeded")
	ErrAccountFrozen        = errors.New("account frozen")
)
