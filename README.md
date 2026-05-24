![Java CI](https://github.com/ramtabog369-hue/realtime-analytics-platform/actions/workflows/java-ci.yml/badge.svg)

# Real-Time Analytics Platform

латформа потоковой обработки и аналитики событий (clickstream) в реальном времени.

## Стек
- Python (Producer, Consumer, Airflow)
- Java (Kafka Consumer)
- Apache Kafka
- Docker, Docker Compose
- Kubernetes (Kind)
- ClickHouse, PostgreSQL, Redis
- GitHub Actions (CI)

## рхитектура
Producer → Kafka → Spark Streaming → ClickHouse / Delta Lake