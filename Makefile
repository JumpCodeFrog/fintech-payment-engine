.PHONY: up down restart logs psql redis test proto help

help:
	@echo "Fintech Payment Engine Commands:"
	@echo "  make up       - Start all infra (Postgres, Redis, Kafka, Jaeger, Prometheus, Grafana)"
	@echo "  make down     - Stop all containers"
	@echo "  make logs     - View docker logs"
	@echo "  make psql     - Open PostgreSQL shell"
	@echo "  make redis    - Open Redis CLI"
	@echo "  make test     - Run unit & integration tests"

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
