package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	paymentv1 "github.com/JumpCodeFrog/fintech-payment-engine/api/proto/payment/v1"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultServerAddress = "localhost:50051"
	aliceUSDAccount      = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0001"
	bobUSDAccount        = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbb0001"

	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
)

type balance struct {
	owner    string
	account  string
	currency string
	amount   decimal.Decimal
}

type transfer struct {
	fromName string
	fromID   string
	toName   string
	toID     string
	amount   decimal.Decimal
}

type transferResult struct {
	transfer    transfer
	transaction string
	err         error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s%sDemo failed:%s %v\n", colorBold, colorRed, colorReset, err)
		os.Exit(1)
	}
}

func run() error {
	address := os.Getenv("GRPC_ADDRESS")
	if address == "" {
		address = defaultServerAddress
	}

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "%sWARN%s close gRPC connection: %v\n", colorYellow, colorReset, err)
		}
	}()

	client := paymentv1.NewPaymentServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("%s%sFintech Payment Engine - Parallel Transfer Demo%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("Server: %s\n\n", address)

	before, err := loadBalances(ctx, client)
	if err != nil {
		return err
	}
	printBalances("BEFORE", before)

	transfers := []transfer{
		{fromName: "Alice", fromID: aliceUSDAccount, toName: "Bob", toID: bobUSDAccount, amount: decimal.NewFromInt(100)},
		{fromName: "Alice", fromID: aliceUSDAccount, toName: "Bob", toID: bobUSDAccount, amount: decimal.RequireFromString("125.50")},
		{fromName: "Bob", fromID: bobUSDAccount, toName: "Alice", toID: aliceUSDAccount, amount: decimal.NewFromInt(40)},
		{fromName: "Bob", fromID: bobUSDAccount, toName: "Alice", toID: aliceUSDAccount, amount: decimal.RequireFromString("15.25")},
	}

	results := executeTransfers(ctx, client, transfers)
	fmt.Printf("%s%sPARALLEL TRANSFERS%s\n", colorBold, colorYellow, colorReset)
	var failedTransfers int
	for result := range results {
		if result.err != nil {
			failedTransfers++
			fmt.Printf("  %sFAIL%s %-5s -> %-5s %8s USD: %v\n",
				colorRed, colorReset,
				result.transfer.fromName, result.transfer.toName,
				result.transfer.amount.StringFixed(2), result.err,
			)
			continue
		}
		fmt.Printf("  %sOK%s   %-5s -> %-5s %8s USD  tx=%s\n",
			colorGreen, colorReset,
			result.transfer.fromName, result.transfer.toName,
			result.transfer.amount.StringFixed(2), result.transaction,
		)
	}
	if failedTransfers > 0 {
		return fmt.Errorf("%d of %d transfers failed", failedTransfers, len(transfers))
	}

	after, err := loadBalances(ctx, client)
	if err != nil {
		return err
	}
	fmt.Println()
	printBalances("AFTER", after)

	beforeTotal := total(before)
	afterTotal := total(after)
	fmt.Printf("%s%sTOTAL MONEY INVARIANT%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("  Before: %s USD\n", beforeTotal.StringFixed(2))
	fmt.Printf("  After:  %s USD\n", afterTotal.StringFixed(2))
	if !beforeTotal.Equal(afterTotal) {
		return fmt.Errorf("money invariant violated: before=%s after=%s", beforeTotal, afterTotal)
	}
	fmt.Printf("  %s%sPASS%s: no money was created or destroyed\n", colorBold, colorGreen, colorReset)
	return nil
}

func loadBalances(ctx context.Context, client paymentv1.PaymentServiceClient) ([]balance, error) {
	accounts := []struct {
		owner string
		id    string
	}{
		{owner: "Alice", id: aliceUSDAccount},
		{owner: "Bob", id: bobUSDAccount},
	}

	balances := make([]balance, 0, len(accounts))
	for _, account := range accounts {
		response, err := client.GetAccountBalance(ctx, &paymentv1.GetAccountBalanceRequest{AccountId: account.id})
		if err != nil {
			return nil, fmt.Errorf("get %s balance: %w", account.owner, err)
		}
		amount, err := decimal.NewFromString(response.GetAvailableBalance())
		if err != nil {
			return nil, fmt.Errorf("parse %s balance: %w", account.owner, err)
		}
		balances = append(balances, balance{
			owner:    account.owner,
			account:  response.GetAccountId(),
			currency: response.GetCurrency(),
			amount:   amount,
		})
	}
	return balances, nil
}

func executeTransfers(
	ctx context.Context,
	client paymentv1.PaymentServiceClient,
	transfers []transfer,
) <-chan transferResult {
	results := make(chan transferResult, len(transfers))
	var wg sync.WaitGroup
	for _, item := range transfers {
		wg.Add(1)
		go func(item transfer) {
			defer wg.Done()
			response, err := client.ProcessTransfer(ctx, &paymentv1.ProcessTransferRequest{
				IdempotencyKey:  "demo-" + uuid.NewString(),
				SourceAccountId: item.fromID,
				TargetAccountId: item.toID,
				Amount:          item.amount.String(),
				Currency:        "USD",
			})
			result := transferResult{transfer: item, err: err}
			if response != nil {
				result.transaction = response.GetTransactionId()
			}
			results <- result
		}(item)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

func printBalances(title string, balances []balance) {
	fmt.Printf("%s%s%s%s\n", colorBold, colorCyan, title, colorReset)
	for _, item := range balances {
		fmt.Printf("  %-5s %12s %s  (%s)\n",
			item.owner,
			item.amount.StringFixed(2),
			item.currency,
			item.account,
		)
	}
	fmt.Printf("  %-5s %12s USD\n\n", "Total", total(balances).StringFixed(2))
}

func total(balances []balance) decimal.Decimal {
	result := decimal.Zero
	for _, item := range balances {
		result = result.Add(item.amount)
	}
	return result
}
