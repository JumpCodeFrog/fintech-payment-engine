package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	postgresrepo "github.com/JumpCodeFrog/fintech-payment-engine/internal/repository/postgres"
	"github.com/JumpCodeFrog/fintech-payment-engine/internal/usecase"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

func TestConcurrentTransfersPreserveBalance(t *testing.T) {
	pool := openTestDatabase(t)
	sourceID, targetID := insertAccounts(t, pool, "100.00", "0.00")
	transfer := newTransferUseCase(pool)

	const attempts = 10
	start := make(chan struct{})
	results := make(chan error, attempts)
	var workers sync.WaitGroup
	workers.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer workers.Done()
			<-start
			_, err := transfer.Execute(context.Background(), usecase.TransferInput{
				IdempotencyKey:  fmt.Sprintf("concurrent-%d", i),
				SourceAccountID: sourceID,
				TargetAccountID: targetID,
				Amount:          decimal.NewFromInt(30),
				Currency:        "USD",
			})
			results <- err
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var succeeded, insufficient int
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrInsufficientFunds):
			insufficient++
		default:
			t.Fatalf("unexpected concurrent transfer error: %v", err)
		}
	}
	if succeeded != 3 || insufficient != 7 {
		t.Fatalf("results = %d succeeded, %d insufficient; want 3 and 7", succeeded, insufficient)
	}

	sourceBalance := accountBalance(t, pool, sourceID)
	targetBalance := accountBalance(t, pool, targetID)
	if !sourceBalance.Equal(decimal.NewFromInt(10)) || !targetBalance.Equal(decimal.NewFromInt(90)) {
		t.Fatalf("balances = %s and %s; want 10 and 90", sourceBalance, targetBalance)
	}
	if !sourceBalance.Add(targetBalance).Equal(decimal.NewFromInt(100)) {
		t.Fatalf("total balance changed: %s", sourceBalance.Add(targetBalance))
	}
	assertRowCount(t, pool, "transactions", 3)
	assertRowCount(t, pool, "outbox_events", 3)
}

func TestIdempotentTransferReplay(t *testing.T) {
	pool := openTestDatabase(t)
	sourceID, targetID := insertAccounts(t, pool, "100.00", "0.00")
	transfer := newTransferUseCase(pool)
	input := usecase.TransferInput{
		IdempotencyKey:  "same-request",
		SourceAccountID: sourceID,
		TargetAccountID: targetID,
		Amount:          decimal.NewFromInt(25),
		Currency:        "USD",
	}

	first, err := transfer.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second, err := transfer.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("replayed Execute() error = %v", err)
	}
	if second.TransactionID != first.TransactionID ||
		second.Status != first.Status ||
		second.CreatedAt.Format(time.RFC3339Nano) != first.CreatedAt.Format(time.RFC3339Nano) {
		t.Fatalf("replay = %#v; want original %#v", second, first)
	}

	conflicting := input
	conflicting.Amount = decimal.NewFromInt(26)
	if _, err := transfer.Execute(context.Background(), conflicting); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v; want idempotency conflict", err)
	}

	if got := accountBalance(t, pool, sourceID); !got.Equal(decimal.NewFromInt(75)) {
		t.Fatalf("source balance after replay = %s; want 75", got)
	}
	if got := accountBalance(t, pool, targetID); !got.Equal(decimal.NewFromInt(25)) {
		t.Fatalf("target balance after replay = %s; want 25", got)
	}
	assertRowCount(t, pool, "transactions", 1)
	assertRowCount(t, pool, "outbox_events", 1)
}

func TestPostgresMoneyDomain(t *testing.T) {
	for _, amount := range []string{"0.00001", "1.23456", "100000000000000.0000"} {
		t.Run("rejects "+amount, func(t *testing.T) {
			pool := openTestDatabase(t)
			sourceID, targetID := insertAccounts(t, pool, "100.00", "0.00")
			transfer := newTransferUseCase(pool)
			input := usecase.TransferInput{
				IdempotencyKey:  "invalid-money-domain",
				SourceAccountID: sourceID,
				TargetAccountID: targetID,
				Amount:          decimal.RequireFromString(amount),
				Currency:        "USD",
			}

			for attempt := range 2 {
				if _, err := transfer.Execute(context.Background(), input); !errors.Is(err, domain.ErrInvalidAmount) {
					t.Fatalf("Execute() attempt %d error = %v; want invalid amount", attempt+1, err)
				}
			}
			if got := accountBalance(t, pool, sourceID); !got.Equal(decimal.NewFromInt(100)) {
				t.Fatalf("source balance = %s; want 100", got)
			}
			if got := accountBalance(t, pool, targetID); !got.IsZero() {
				t.Fatalf("target balance = %s; want 0", got)
			}
			assertRowCount(t, pool, "transactions", 0)
			assertRowCount(t, pool, "outbox_events", 0)
		})
	}
}

func TestMaximumPostgresMoneyValueReplaysExactly(t *testing.T) {
	pool := openTestDatabase(t)
	maximum := decimal.RequireFromString("99999999999999.9999")
	sourceID, targetID := insertAccounts(t, pool, maximum.StringFixed(4), "0.0000")
	transfer := newTransferUseCase(pool)
	input := usecase.TransferInput{
		IdempotencyKey:  "maximum-money-value",
		SourceAccountID: sourceID,
		TargetAccountID: targetID,
		Amount:          maximum,
		Currency:        "USD",
	}

	first, err := transfer.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	replay, err := transfer.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("replayed Execute() error = %v", err)
	}
	if replay.TransactionID != first.TransactionID {
		t.Fatalf("replay transaction ID = %s; want %s", replay.TransactionID, first.TransactionID)
	}
	if got := accountBalance(t, pool, sourceID); !got.IsZero() {
		t.Fatalf("source balance = %s; want 0", got)
	}
	if got := accountBalance(t, pool, targetID); !got.Equal(maximum) {
		t.Fatalf("target balance = %s; want %s", got, maximum)
	}
	assertRowCount(t, pool, "transactions", 1)
	assertRowCount(t, pool, "outbox_events", 1)
}

func TestTransferRejectsTargetBalanceOverflow(t *testing.T) {
	pool := openTestDatabase(t)
	maximum := decimal.RequireFromString("99999999999999.9999")
	sourceID, targetID := insertAccounts(t, pool, "1.0000", maximum.StringFixed(4))
	transfer := newTransferUseCase(pool)

	_, err := transfer.Execute(context.Background(), usecase.TransferInput{
		IdempotencyKey:  "target-overflow",
		SourceAccountID: sourceID,
		TargetAccountID: targetID,
		Amount:          decimal.NewFromInt(1),
		Currency:        "USD",
	})
	if !errors.Is(err, domain.ErrBalanceLimitExceeded) {
		t.Fatalf("Execute() error = %v; want balance limit exceeded", err)
	}
	if got := accountBalance(t, pool, sourceID); !got.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("source balance = %s; want 1", got)
	}
	if got := accountBalance(t, pool, targetID); !got.Equal(maximum) {
		t.Fatalf("target balance = %s; want %s", got, maximum)
	}
	assertRowCount(t, pool, "transactions", 0)
	assertRowCount(t, pool, "outbox_events", 0)
}

func TestConcurrentIdempotentTransferReturnsOneResult(t *testing.T) {
	pool := openTestDatabase(t)
	sourceID, targetID := insertAccounts(t, pool, "100.00", "0.00")
	transfer := newTransferUseCase(pool)
	input := usecase.TransferInput{
		IdempotencyKey:  "concurrent-same-request",
		SourceAccountID: sourceID,
		TargetAccountID: targetID,
		Amount:          decimal.NewFromInt(25),
		Currency:        "USD",
	}

	start := make(chan struct{})
	results := make(chan transferResult, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			<-start
			output, err := transfer.Execute(context.Background(), input)
			results <- transferResult{output: output, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	var transactionID uuid.UUID
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent replay error = %v", result.err)
		}
		if result.output == nil {
			t.Fatal("concurrent replay returned nil output")
		}
		if transactionID == uuid.Nil {
			transactionID = result.output.TransactionID
		} else if result.output.TransactionID != transactionID {
			t.Fatalf("transaction IDs differ: %s and %s", transactionID, result.output.TransactionID)
		}
	}

	if got := accountBalance(t, pool, sourceID); !got.Equal(decimal.NewFromInt(75)) {
		t.Fatalf("source balance = %s; want 75", got)
	}
	if got := accountBalance(t, pool, targetID); !got.Equal(decimal.NewFromInt(25)) {
		t.Fatalf("target balance = %s; want 25", got)
	}
	assertRowCount(t, pool, "transactions", 1)
	assertRowCount(t, pool, "outbox_events", 1)
}

type transferResult struct {
	output *usecase.TransferOutput
	err    error
}

func openTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	connectionString := os.Getenv("TEST_POSTGRES_URL")
	if connectionString == "" {
		t.Skip("TEST_POSTGRES_URL is required for PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminPool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatalf("open admin PostgreSQL pool: %v", err)
	}

	schema := "payment_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		adminPool.Close()
		t.Fatalf("create test schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		adminPool.Close()
		t.Fatalf("parse PostgreSQL URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		adminPool.Close()
		t.Fatalf("open schema PostgreSQL pool: %v", err)
	}

	migrationPath := filepath.Join("..", "..", "migrations", "000001_init_schema.up.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		pool.Close()
		adminPool.Close()
		t.Fatalf("read schema migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		pool.Close()
		adminPool.Close()
		t.Fatalf("apply schema migration: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := adminPool.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		adminPool.Close()
	})
	return pool
}

func insertAccounts(t *testing.T, pool *pgxpool.Pool, sourceBalance, targetBalance string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	sourceID := uuid.New()
	targetID := uuid.New()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO accounts (id, user_id, currency, balance, frozen_balance, version)
		VALUES ($1, $2, 'USD', $3, 0, 1), ($4, $5, 'USD', $6, 0, 1)`,
		sourceID, uuid.New(), sourceBalance, targetID, uuid.New(), targetBalance,
	)
	if err != nil {
		t.Fatalf("insert test accounts: %v", err)
	}
	return sourceID, targetID
}

func newTransferUseCase(pool *pgxpool.Pool) *usecase.TransferUseCase {
	return usecase.NewTransferUseCase(
		postgresrepo.NewAccountRepository(pool),
		postgresrepo.NewTransactionRepository(pool),
		postgresrepo.NewOutboxRepository(pool),
		postgresrepo.NewTransactor(pool),
	)
}

func accountBalance(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) decimal.Decimal {
	t.Helper()
	var balance string
	if err := pool.QueryRow(context.Background(), "SELECT balance::text FROM accounts WHERE id = $1", accountID).Scan(&balance); err != nil {
		t.Fatalf("read account balance: %v", err)
	}
	return decimal.RequireFromString(balance)
}

func assertRowCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&got); err != nil {
		t.Fatalf("count %s rows: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s rows = %d; want %d", table, got, want)
	}
}
