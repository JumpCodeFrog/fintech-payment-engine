package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	paymentv1 "github.com/JumpCodeFrog/fintech-payment-engine/api/proto/payment/v1"
	deliverygrpc "github.com/JumpCodeFrog/fintech-payment-engine/internal/delivery/grpc"
	postgresrepo "github.com/JumpCodeFrog/fintech-payment-engine/internal/repository/postgres"
	"github.com/JumpCodeFrog/fintech-payment-engine/internal/usecase"
	"github.com/JumpCodeFrog/fintech-payment-engine/pkg/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("API server stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.Postgres.URL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}

	accountRepo := postgresrepo.NewAccountRepository(pool)
	transactionRepo := postgresrepo.NewTransactionRepository(pool)
	outboxRepo := postgresrepo.NewOutboxRepository(pool)
	transactor := postgresrepo.NewTransactor(pool)
	transfer := usecase.NewTransferUseCase(accountRepo, transactionRepo, outboxRepo, transactor)
	handler := deliverygrpc.NewPaymentHandler(transfer, accountRepo, transactionRepo, logger)

	listener, err := net.Listen("tcp", cfg.GRPC.Port)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := grpc.NewServer()
	paymentv1.RegisterPaymentServiceServer(server, handler)
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("gRPC server started", "address", cfg.GRPC.Port)
		serveErr <- server.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownDone := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		logger.Warn("gRPC graceful shutdown timed out")
		server.Stop()
	}
	logger.Info("gRPC server stopped")
	return nil
}
