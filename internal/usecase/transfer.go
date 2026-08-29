package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type TransferInput struct {
	IdempotencyKey  string
	SourceAccountID uuid.UUID
	TargetAccountID uuid.UUID
	Amount          decimal.Decimal
	Currency        string
}

type TransferOutput struct {
	TransactionID uuid.UUID
	Status        domain.TransactionStatus
	CreatedAt     time.Time
}

type TransferUseCase struct {
	accounts     domain.AccountRepository
	transactions domain.TransactionRepository
	outbox       domain.OutboxRepository
	transactor   domain.Transactor
}

func NewTransferUseCase(
	accounts domain.AccountRepository,
	transactions domain.TransactionRepository,
	outbox domain.OutboxRepository,
	transactor domain.Transactor,
) *TransferUseCase {
	return &TransferUseCase{
		accounts:     accounts,
		transactions: transactions,
		outbox:       outbox,
		transactor:   transactor,
	}
}

func (uc *TransferUseCase) Execute(ctx context.Context, input TransferInput) (*TransferOutput, error) {
	if !input.Amount.GreaterThan(decimal.Zero) {
		return nil, domain.ErrInvalidAmount
	}
	if input.SourceAccountID == input.TargetAccountID {
		return nil, domain.ErrInvalidAmount
	}

	var output *TransferOutput
	err := uc.transactor.WithinTransaction(ctx, func(txCtx context.Context) error {
		existing, err := uc.transactions.GetByIdempotencyKey(txCtx, input.IdempotencyKey)
		if err != nil {
			return fmt.Errorf("get transaction by idempotency key: %w", err)
		}
		if existing != nil {
			return domain.ErrIdempotencyConflict
		}

		firstID, secondID := input.SourceAccountID, input.TargetAccountID
		if bytes.Compare(firstID[:], secondID[:]) > 0 {
			firstID, secondID = secondID, firstID
		}

		firstAccount, err := uc.accounts.GetByIDForUpdate(txCtx, firstID)
		if err != nil {
			return fmt.Errorf("lock account %s: %w", firstID, err)
		}
		if firstAccount == nil {
			return domain.ErrAccountNotFound
		}

		secondAccount, err := uc.accounts.GetByIDForUpdate(txCtx, secondID)
		if err != nil {
			return fmt.Errorf("lock account %s: %w", secondID, err)
		}
		if secondAccount == nil {
			return domain.ErrAccountNotFound
		}

		source, target := firstAccount, secondAccount
		if firstID != input.SourceAccountID {
			source, target = secondAccount, firstAccount
		}

		if source.Currency != input.Currency || target.Currency != input.Currency {
			return domain.ErrCurrencyMismatch
		}
		if source.Balance.LessThan(input.Amount) {
			return domain.ErrInsufficientFunds
		}

		now := time.Now().UTC()
		source.Balance = source.Balance.Sub(input.Amount)
		source.UpdatedAt = now
		target.Balance = target.Balance.Add(input.Amount)
		target.UpdatedAt = now

		if err := uc.accounts.Update(txCtx, source); err != nil {
			return fmt.Errorf("update source account: %w", err)
		}
		if err := uc.accounts.Update(txCtx, target); err != nil {
			return fmt.Errorf("update target account: %w", err)
		}

		transactionID := uuid.New()
		transaction := &domain.Transaction{
			ID:              transactionID,
			IdempotencyKey:  input.IdempotencyKey,
			SourceAccountID: &input.SourceAccountID,
			TargetAccountID: &input.TargetAccountID,
			Amount:          input.Amount,
			Currency:        input.Currency,
			Status:          domain.TransactionStatusCompleted,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if err := uc.transactions.Create(txCtx, transaction); err != nil {
			return fmt.Errorf("create transaction: %w", err)
		}

		payload, err := json.Marshal(struct {
			TransactionID   uuid.UUID       `json:"transaction_id"`
			SourceAccountID uuid.UUID       `json:"source_account_id"`
			TargetAccountID uuid.UUID       `json:"target_account_id"`
			Amount          decimal.Decimal `json:"amount"`
			Currency        string          `json:"currency"`
			TransferredAt   time.Time       `json:"transferred_at"`
		}{
			TransactionID:   transactionID,
			SourceAccountID: input.SourceAccountID,
			TargetAccountID: input.TargetAccountID,
			Amount:          input.Amount,
			Currency:        input.Currency,
			TransferredAt:   now,
		})
		if err != nil {
			return fmt.Errorf("serialize outbox payload: %w", err)
		}

		event := &domain.OutboxEvent{
			ID:            uuid.New(),
			AggregateType: "payment",
			AggregateID:   transactionID.String(),
			EventType:     "payment.transferred",
			Payload:       payload,
			Status:        domain.OutboxStatusPending,
			CreatedAt:     now,
		}
		if err := uc.outbox.Create(txCtx, event); err != nil {
			return fmt.Errorf("create outbox event: %w", err)
		}

		output = &TransferOutput{
			TransactionID: transactionID,
			Status:        transaction.Status,
			CreatedAt:     now,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return output, nil
}
