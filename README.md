# 🥇 fintech-payment-engine

> High-throughput, resilient distributed payment gateway built with **Go (1.22+)**, **gRPC**, **PostgreSQL**, **Kafka**, and **OpenTelemetry**.

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-316192?style=for-the-badge&logo=postgresql)](https://www.postgresql.org)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-E03C31?style=for-the-badge&logo=apachekafka)](https://redpanda.com)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis)](https://redis.io)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Tracing-F5A800?style=for-the-badge&logo=opentelemetry)](https://opentelemetry.io)

---

## 🏛️ Architecture Overview

The service guarantees **Zero Double-Spending** and **At-Least-Once Event Delivery** through:
- **Clean Architecture & Domain-Driven Design (DDD)**
- **Transactional Outbox Pattern** with `SELECT FOR UPDATE SKIP LOCKED`
- **Distributed Idempotency Layer** (Redis Redlock + Unique Constraints)
- **Distributed Tracing & Metrics** (OTel, Jaeger, Prometheus, Grafana)

```
fintech-payment-engine/
├── api/proto/payment/v1/          # Protobuf / gRPC Contracts
├── cmd/
│   ├── api/                       # API Server entrypoint
│   └── outbox-worker/             # Background Kafka event dispatcher
├── internal/
│   ├── domain/                    # Business entities & errors
│   ├── usecase/                   # Payment & transfer logic
│   ├── repository/                # Postgres & Redis data access
│   └── delivery/                  # gRPC & HTTP handlers
├── pkg/
│   ├── telemetry/                 # OpenTelemetry & Prometheus setup
│   └── postgres/                  # pgxpool connection & migrations
├── deployments/
│   ├── docker-compose.yml         # Full infra stack
│   └── prometheus.yml             # Metrics scrape config
└── migrations/                    # SQL schema migrations
```

---

## 🚀 Quick Start (One Command)

### 1. Start Infrastructure
```bash
docker compose -f deployments/docker-compose.yml up -d
```

### 2. Available Dashboards & Services:
| Service | URL | Description |
|---|---|---|
| **PostgreSQL** | `localhost:5432` | DB: `fintech_db` (user: `postgres`, pass: `password`) |
| **Kafka UI (Redpanda Console)** | [http://localhost:8080](http://localhost:8080) | Topic inspection & message browser |
| **Jaeger UI** | [http://localhost:16686](http://localhost:16686) | Distributed Tracing & Latency waterfall |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | Metrics & Alerting |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | Live Dashboards (admin/admin) |
| **Redis** | `localhost:6379` | Cache & Distributed Locks |

---

## 📄 License
MIT © Thomas ([@JumpCodeFrog](https://github.com/JumpCodeFrog))
