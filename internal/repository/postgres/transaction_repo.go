package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type TransactionRepository struct {
	pool *pgxpool.Pool
}

func NewTransactionRepository(pool *pgxpool.Pool) *TransactionRepository {
	return &TransactionRepository{pool: pool}
}

func (r *TransactionRepository) Create(ctx context.Context, transaction *domain.Transaction) error {
	_, err := dbtx(ctx, r.pool).Exec(ctx, `
		INSERT INTO transactions (
			id, idempotency_key, source_account_id, target_account_id,
			amount, currency, status, error_reason, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9, $10)`,
		transaction.ID,
		transaction.IdempotencyKey,
		transaction.SourceAccountID,
		transaction.TargetAccountID,
		transaction.Amount.String(),
		transaction.Currency,
		transaction.Status,
		transaction.ErrorReason,
		transaction.CreatedAt,
		transaction.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "transactions_idempotency_key_key" {
			return domain.ErrIdempotencyConflict
		}
		return fmt.Errorf("insert transaction: %w", err)
	}
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Transaction, error) {
	return r.get(ctx, `
		SELECT id, idempotency_key, source_account_id, target_account_id,
		       amount::text, currency, status, COALESCE(error_reason, ''),
		       created_at, updated_at
		FROM transactions
		WHERE id = $1`, id)
}

func (r *TransactionRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Transaction, error) {
	return r.get(ctx, `
		SELECT id, idempotency_key, source_account_id, target_account_id,
		       amount::text, currency, status, COALESCE(error_reason, ''),
		       created_at, updated_at
		FROM transactions
		WHERE idempotency_key = $1`, key)
}

func (r *TransactionRepository) get(ctx context.Context, query string, argument any) (*domain.Transaction, error) {
	var transaction domain.Transaction
	var sourceAccountID, targetAccountID pgtype.UUID
	var amount string
	err := dbtx(ctx, r.pool).QueryRow(ctx, query, argument).Scan(
		&transaction.ID,
		&transaction.IdempotencyKey,
		&sourceAccountID,
		&targetAccountID,
		&amount,
		&transaction.Currency,
		&transaction.Status,
		&transaction.ErrorReason,
		&transaction.CreatedAt,
		&transaction.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select transaction: %w", err)
	}

	if sourceAccountID.Valid {
		id := uuid.UUID(sourceAccountID.Bytes)
		transaction.SourceAccountID = &id
	}
	if targetAccountID.Valid {
		id := uuid.UUID(targetAccountID.Bytes)
		transaction.TargetAccountID = &id
	}
	transaction.Amount, err = decimal.NewFromString(amount)
	if err != nil {
		return nil, fmt.Errorf("parse transaction amount: %w", err)
	}

	return &transaction, nil
}

func (r *TransactionRepository) Update(ctx context.Context, transaction *domain.Transaction) error {
	result, err := dbtx(ctx, r.pool).Exec(ctx, `
		UPDATE transactions
		SET status = $2, error_reason = NULLIF($3, ''), updated_at = $4
		WHERE id = $1`,
		transaction.ID,
		transaction.Status,
		transaction.ErrorReason,
		transaction.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update transaction: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("update transaction %s: not found", transaction.ID)
	}
	return nil
}

var _ domain.TransactionRepository = (*TransactionRepository)(nil)
