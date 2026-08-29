package grpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	paymentv1 "github.com/JumpCodeFrog/fintech-payment-engine/api/proto/payment/v1"
	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/JumpCodeFrog/fintech-payment-engine/internal/usecase"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PaymentHandler struct {
	paymentv1.UnimplementedPaymentServiceServer
	transfer     *usecase.TransferUseCase
	accounts     domain.AccountRepository
	transactions domain.TransactionRepository
	logger       *slog.Logger
}

func NewPaymentHandler(
	transfer *usecase.TransferUseCase,
	accounts domain.AccountRepository,
	transactions domain.TransactionRepository,
	logger *slog.Logger,
) *PaymentHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &PaymentHandler{
		transfer:     transfer,
		accounts:     accounts,
		transactions: transactions,
		logger:       logger,
	}
}

func (h *PaymentHandler) ProcessTransfer(
	ctx context.Context,
	req *paymentv1.ProcessTransferRequest,
) (*paymentv1.ProcessTransferResponse, error) {
	input, err := parseTransferRequest(req)
	if err != nil {
		return nil, err
	}

	output, err := h.transfer.Execute(ctx, input)
	if err != nil {
		return nil, h.mapError(ctx, err)
	}

	return &paymentv1.ProcessTransferResponse{
		TransactionId: output.TransactionID.String(),
		Status:        string(output.Status),
		CreatedAt:     output.CreatedAt.Format(time.RFC3339Nano),
	}, nil
}

func (h *PaymentHandler) GetAccountBalance(
	ctx context.Context,
	req *paymentv1.GetAccountBalanceRequest,
) (*paymentv1.GetAccountBalanceResponse, error) {
	if req == nil || strings.TrimSpace(req.GetAccountId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	accountID, err := uuid.Parse(req.GetAccountId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "account_id must be a valid UUID")
	}

	account, err := h.accounts.GetByID(ctx, accountID)
	if err != nil {
		return nil, h.mapError(ctx, err)
	}
	if account == nil {
		return nil, status.Error(codes.NotFound, "account not found")
	}

	return &paymentv1.GetAccountBalanceResponse{
		AccountId:        account.ID.String(),
		Currency:         account.Currency,
		AvailableBalance: account.Balance.String(),
		FrozenBalance:    account.FrozenBalance.String(),
	}, nil
}

func (h *PaymentHandler) GetTransactionStatus(
	ctx context.Context,
	req *paymentv1.GetTransactionStatusRequest,
) (*paymentv1.GetTransactionStatusResponse, error) {
	if req == nil || strings.TrimSpace(req.GetIdempotencyKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	transaction, err := h.transactions.GetByIdempotencyKey(ctx, req.GetIdempotencyKey())
	if err != nil {
		return nil, h.mapError(ctx, err)
	}
	if transaction == nil {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}

	return &paymentv1.GetTransactionStatusResponse{
		TransactionId: transaction.ID.String(),
		Status:        string(transaction.Status),
		ErrorReason:   transaction.ErrorReason,
	}, nil
}

func parseTransferRequest(req *paymentv1.ProcessTransferRequest) (usecase.TransferInput, error) {
	if req == nil {
		return usecase.TransferInput{}, status.Error(codes.InvalidArgument, "request is required")
	}
	if strings.TrimSpace(req.GetIdempotencyKey()) == "" {
		return usecase.TransferInput{}, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	sourceAccountID, err := uuid.Parse(req.GetSourceAccountId())
	if err != nil {
		return usecase.TransferInput{}, status.Error(codes.InvalidArgument, "source_account_id must be a valid UUID")
	}
	targetAccountID, err := uuid.Parse(req.GetTargetAccountId())
	if err != nil {
		return usecase.TransferInput{}, status.Error(codes.InvalidArgument, "target_account_id must be a valid UUID")
	}
	amount, err := decimal.NewFromString(req.GetAmount())
	if err != nil || !amount.GreaterThan(decimal.Zero) {
		return usecase.TransferInput{}, status.Error(codes.InvalidArgument, "amount must be a positive decimal")
	}
	currency := strings.ToUpper(strings.TrimSpace(req.GetCurrency()))
	if len(currency) != 3 {
		return usecase.TransferInput{}, status.Error(codes.InvalidArgument, "currency must be a 3-letter code")
	}

	return usecase.TransferInput{
		IdempotencyKey:  strings.TrimSpace(req.GetIdempotencyKey()),
		SourceAccountID: sourceAccountID,
		TargetAccountID: targetAccountID,
		Amount:          amount,
		Currency:        currency,
	}, nil
}

func (h *PaymentHandler) mapError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, domain.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, domain.ErrInsufficientFunds.Error())
	case errors.Is(err, domain.ErrAccountNotFound):
		return status.Error(codes.NotFound, domain.ErrAccountNotFound.Error())
	case errors.Is(err, domain.ErrInvalidAmount), errors.Is(err, domain.ErrCurrencyMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, domain.ErrIdempotencyConflict.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		h.logger.ErrorContext(ctx, "payment request failed", "error", err)
		return status.Error(codes.Internal, "internal server error")
	}
}

var _ paymentv1.PaymentServiceServer = (*PaymentHandler)(nil)
