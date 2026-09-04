package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestTransferUseCaseExecute(t *testing.T) {
	smallerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	largerID := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	tests := []struct {
		name            string
		input           TransferInput
		accounts        map[uuid.UUID]*domain.Account
		wantErr         error
		wantSource      string
		wantTarget      string
		wantWrites      bool
		wantLockedOrder []uuid.UUID
	}{
		{
			name: "successful transfer",
			input: TransferInput{
				IdempotencyKey:  "transfer-1",
				SourceAccountID: largerID,
				TargetAccountID: smallerID,
				Amount:          decimal.RequireFromString("25.50000"),
				Currency:        "USD",
			},
			accounts: map[uuid.UUID]*domain.Account{
				largerID:  {ID: largerID, Currency: "USD", Balance: decimal.NewFromInt(100), Version: 1},
				smallerID: {ID: smallerID, Currency: "USD", Balance: decimal.NewFromInt(10), Version: 1},
			},
			wantSource:      "74.50",
			wantTarget:      "35.50",
			wantWrites:      true,
			wantLockedOrder: []uuid.UUID{smallerID, largerID},
		},
		{
			name: "insufficient funds",
			input: TransferInput{
				IdempotencyKey:  "transfer-2",
				SourceAccountID: smallerID,
				TargetAccountID: largerID,
				Amount:          decimal.NewFromInt(20),
				Currency:        "USD",
			},
			accounts: map[uuid.UUID]*domain.Account{
				smallerID: {ID: smallerID, Currency: "USD", Balance: decimal.NewFromInt(10)},
				largerID:  {ID: largerID, Currency: "USD", Balance: decimal.NewFromInt(100)},
			},
			wantErr:         domain.ErrInsufficientFunds,
			wantLockedOrder: []uuid.UUID{smallerID, largerID},
		},
		{
			name: "currency mismatch",
			input: TransferInput{
				IdempotencyKey:  "transfer-3",
				SourceAccountID: smallerID,
				TargetAccountID: largerID,
				Amount:          decimal.NewFromInt(10),
				Currency:        "USD",
			},
			accounts: map[uuid.UUID]*domain.Account{
				smallerID: {ID: smallerID, Currency: "USD", Balance: decimal.NewFromInt(100)},
				largerID:  {ID: largerID, Currency: "EUR", Balance: decimal.NewFromInt(100)},
			},
			wantErr:         domain.ErrCurrencyMismatch,
			wantLockedOrder: []uuid.UUID{smallerID, largerID},
		},
		{
			name: "amount must be positive",
			input: TransferInput{
				IdempotencyKey:  "transfer-4",
				SourceAccountID: smallerID,
				TargetAccountID: largerID,
				Amount:          decimal.Zero,
				Currency:        "USD",
			},
			wantErr: domain.ErrInvalidAmount,
		},
		{
			name: "amount below database scale",
			input: TransferInput{
				IdempotencyKey:  "transfer-too-small",
				SourceAccountID: smallerID,
				TargetAccountID: largerID,
				Amount:          decimal.RequireFromString("0.00001"),
				Currency:        "USD",
			},
			wantErr: domain.ErrInvalidAmount,
		},
		{
			name: "amount requires database rounding",
			input: TransferInput{
				IdempotencyKey:  "transfer-too-precise",
				SourceAccountID: smallerID,
				TargetAccountID: largerID,
				Amount:          decimal.RequireFromString("1.23456"),
				Currency:        "USD",
			},
			wantErr: domain.ErrInvalidAmount,
		},
		{
			name: "amount exceeds database precision",
			input: TransferInput{
				IdempotencyKey:  "transfer-too-large",
				SourceAccountID: smallerID,
				TargetAccountID: largerID,
				Amount:          decimal.RequireFromString("100000000000000.0000"),
				Currency:        "USD",
			},
			wantErr: domain.ErrInvalidAmount,
		},
		{
			name: "accounts must differ",
			input: TransferInput{
				IdempotencyKey:  "transfer-5",
				SourceAccountID: smallerID,
				TargetAccountID: smallerID,
				Amount:          decimal.NewFromInt(10),
				Currency:        "USD",
			},
			wantErr: domain.ErrInvalidAmount,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accounts := &accountRepositoryStub{accounts: tt.accounts}
			transactions := &transactionRepositoryStub{}
			outbox := &outboxRepositoryStub{}
			transactor := &transactorStub{}
			uc := NewTransferUseCase(accounts, transactions, outbox, transactor)

			output, err := uc.Execute(context.Background(), tt.input)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if output != nil {
					t.Fatalf("Execute() output = %#v, want nil", output)
				}
				if len(transactions.created) != 0 || len(outbox.created) != 0 {
					t.Fatal("business error must not create transaction or outbox event")
				}
			} else {
				assertSuccessfulTransfer(t, output, tt, accounts, transactions, outbox)
			}

			if len(accounts.lockedIDs) != len(tt.wantLockedOrder) {
				t.Fatalf("locked IDs = %v, want %v", accounts.lockedIDs, tt.wantLockedOrder)
			}
			for i := range tt.wantLockedOrder {
				if accounts.lockedIDs[i] != tt.wantLockedOrder[i] {
					t.Fatalf("locked IDs = %v, want %v", accounts.lockedIDs, tt.wantLockedOrder)
				}
			}

			wantTransactionCalls := 1
			if tt.wantErr == domain.ErrInvalidAmount {
				wantTransactionCalls = 0
			}
			if transactor.calls != wantTransactionCalls {
				t.Fatalf("transactor calls = %d, want %d", transactor.calls, wantTransactionCalls)
			}
		})
	}
}

func assertSuccessfulTransfer(
	t *testing.T,
	output *TransferOutput,
	tt struct {
		name            string
		input           TransferInput
		accounts        map[uuid.UUID]*domain.Account
		wantErr         error
		wantSource      string
		wantTarget      string
		wantWrites      bool
		wantLockedOrder []uuid.UUID
	},
	accounts *accountRepositoryStub,
	transactions *transactionRepositoryStub,
	outbox *outboxRepositoryStub,
) {
	t.Helper()
	if output == nil || output.Status != domain.TransactionStatusCompleted {
		t.Fatalf("Execute() output = %#v, want completed transfer", output)
	}
	if got := accounts.accounts[tt.input.SourceAccountID].Balance.StringFixed(2); got != tt.wantSource {
		t.Errorf("source balance = %s, want %s", got, tt.wantSource)
	}
	if got := accounts.accounts[tt.input.TargetAccountID].Balance.StringFixed(2); got != tt.wantTarget {
		t.Errorf("target balance = %s, want %s", got, tt.wantTarget)
	}
	if len(accounts.updated) != 2 {
		t.Fatalf("updated accounts = %d, want 2", len(accounts.updated))
	}
	if len(transactions.created) != 1 {
		t.Fatalf("created transactions = %d, want 1", len(transactions.created))
	}
	if transactions.created[0].ID != output.TransactionID || transactions.created[0].Status != domain.TransactionStatusCompleted {
		t.Errorf("created transaction does not match output: %#v", transactions.created[0])
	}
	if len(outbox.created) != 1 {
		t.Fatalf("created outbox events = %d, want 1", len(outbox.created))
	}
	event := outbox.created[0]
	if event.EventType != "payment.transferred" || event.Status != domain.OutboxStatusPending {
		t.Errorf("outbox event = %#v, want pending payment.transferred", event)
	}
	var payload struct {
		TransactionID uuid.UUID `json:"transaction_id"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("unmarshal outbox payload: %v", err)
	}
	if payload.TransactionID != output.TransactionID {
		t.Errorf("payload transaction ID = %s, want %s", payload.TransactionID, output.TransactionID)
	}
}

type accountRepositoryStub struct {
	accounts  map[uuid.UUID]*domain.Account
	lockedIDs []uuid.UUID
	updated   []*domain.Account
}

func (r *accountRepositoryStub) GetByID(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	return r.accounts[id], nil
}

func (r *accountRepositoryStub) GetByIDForUpdate(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	r.lockedIDs = append(r.lockedIDs, id)
	return r.accounts[id], nil
}

func (r *accountRepositoryStub) Update(_ context.Context, account *domain.Account) error {
	r.updated = append(r.updated, account)
	return nil
}

type transactionRepositoryStub struct {
	created  []*domain.Transaction
	existing *domain.Transaction
}

func (r *transactionRepositoryStub) Create(_ context.Context, transaction *domain.Transaction) error {
	r.created = append(r.created, transaction)
	return nil
}

func (r *transactionRepositoryStub) GetByID(_ context.Context, _ uuid.UUID) (*domain.Transaction, error) {
	return nil, nil
}

func (r *transactionRepositoryStub) GetByIdempotencyKey(_ context.Context, _ string) (*domain.Transaction, error) {
	return r.existing, nil
}

func (r *transactionRepositoryStub) Update(_ context.Context, _ *domain.Transaction) error {
	return nil
}

type outboxRepositoryStub struct {
	created []*domain.OutboxEvent
}

func (r *outboxRepositoryStub) Create(_ context.Context, event *domain.OutboxEvent) error {
	r.created = append(r.created, event)
	return nil
}

func (r *outboxRepositoryStub) GetPending(_ context.Context, _ int) ([]*domain.OutboxEvent, error) {
	return nil, nil
}

func (r *outboxRepositoryStub) Update(_ context.Context, _ *domain.OutboxEvent) error {
	return nil
}

type transactorStub struct {
	calls int
}

func (t *transactorStub) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	t.calls++
	return fn(ctx)
}
