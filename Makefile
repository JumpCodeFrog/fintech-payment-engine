.PHONY: up down restart logs psql redis migrate test test-race demo proto help

help:
	@echo "Fintech Payment Engine Commands:"
	@echo "  make up       - Start all infra (Postgres, Redis, Kafka, Jaeger, Prometheus, Grafana)"
	@echo "  make down     - Stop all containers"
	@echo "  make logs     - View docker logs"
	@echo "  make psql     - Open PostgreSQL shell"
	@echo "  make redis    - Open Redis CLI"
	@echo "  make migrate  - Apply all PostgreSQL up migrations"
	@echo "  make test     - Run unit & integration tests"
	@echo "  make test-race - Run tests with the race detector"
	@echo "  make demo     - Run the gRPC transfer demo"

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

restart: down up

logs:
	docker compose -f deployments/docker-compose.yml logs -f

psql:
	docker exec -it fintech_postgres psql -U postgres -d fintech_db

redis:
	docker exec -it fintech_redis redis-cli

migrate:
	@for migration in migrations/*.up.sql; do \
		echo "Applying $$migration"; \
		docker exec -i fintech_postgres psql -v ON_ERROR_STOP=1 -U postgres -d fintech_db < "$$migration" || exit 1; \
	done

test:
	go test ./...

test-race:
	go test -v -race ./...

demo:
	go run ./cmd/demo
