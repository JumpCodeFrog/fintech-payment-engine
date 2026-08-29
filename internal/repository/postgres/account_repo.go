package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type AccountRepository struct {
	pool *pgxpool.Pool
}

func NewAccountRepository(pool *pgxpool.Pool) *AccountRepository {
	return &AccountRepository{pool: pool}
}

func (r *AccountRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return r.getByID(ctx, id, false)
}

func (r *AccountRepository) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return r.getByID(ctx, id, true)
}

func (r *AccountRepository) getByID(ctx context.Context, id uuid.UUID, forUpdate bool) (*domain.Account, error) {
	query := `
		SELECT id, user_id, currency, balance::text, frozen_balance::text,
		       version, created_at, updated_at
		FROM accounts
		WHERE id = $1`
	if forUpdate {
		query += " FOR UPDATE"
	}

	var account domain.Account
	var balance, frozenBalance string
	err := dbtx(ctx, r.pool).QueryRow(ctx, query, id).Scan(
		&account.ID,
		&account.UserID,
		&account.Currency,
		&balance,
		&frozenBalance,
		&account.Version,
		&account.CreatedAt,
		&account.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select account: %w", err)
	}

	account.Balance, err = decimal.NewFromString(balance)
	if err != nil {
		return nil, fmt.Errorf("parse account balance: %w", err)
	}
	account.FrozenBalance, err = decimal.NewFromString(frozenBalance)
	if err != nil {
		return nil, fmt.Errorf("parse account frozen balance: %w", err)
	}

	return &account, nil
}

func (r *AccountRepository) Update(ctx context.Context, account *domain.Account) error {
	result, err := dbtx(ctx, r.pool).Exec(ctx, `
		UPDATE accounts
		SET balance = $3,
		    frozen_balance = $4,
		    version = version + 1,
		    updated_at = $5
		WHERE id = $1 AND version = $2`,
		account.ID,
		account.Version,
		account.Balance.String(),
		account.FrozenBalance.String(),
		account.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update account: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("update account %s: optimistic lock conflict", account.ID)
	}

	account.Version++
	return nil
}

var _ domain.AccountRepository = (*AccountRepository)(nil)
