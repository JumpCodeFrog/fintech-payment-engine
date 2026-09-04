TEST_POSTGRES_URL ?= postgres://postgres:password@localhost:5432/fintech_db?sslmode=disable

.PHONY: up down restart logs psql migrate test test-race test-integration demo help

help:
	@echo "Fintech Payment Engine Commands:"
	@echo "  make up               - Start PostgreSQL, Redpanda, and Kafka topic"
	@echo "  make down             - Stop local infrastructure"
	@echo "  make logs             - View infrastructure logs"
	@echo "  make psql             - Open PostgreSQL shell"
	@echo "  make migrate          - Apply all PostgreSQL up migrations"
	@echo "  make test             - Run tests without external services"
	@echo "  make test-race        - Run tests with the race detector"
	@echo "  make test-integration - Run real PostgreSQL tests with the race detector"
	@echo "  make demo             - Run the gRPC transfer demo"

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

restart: down up

logs:
	docker compose -f deployments/docker-compose.yml logs -f

psql:
	docker exec -it fintech_postgres psql -U postgres -d fintech_db


migrate:
	@for migration in migrations/*.up.sql; do \
		echo "Applying $$migration"; \
		docker exec -i fintech_postgres psql -v ON_ERROR_STOP=1 -U postgres -d fintech_db < "$$migration" || exit 1; \
	done

test:
	go test ./...

test-race:
	go test -v -race ./...

test-integration:
	TEST_POSTGRES_URL='$(TEST_POSTGRES_URL)' go test -v -race ./internal/integration -count=1

demo:
	go run ./cmd/demo
