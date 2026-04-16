.PHONY: help setup ingest-batch ingest-stream transform-silver transform-gold \
        run-dq reconcile test lint docker-up docker-down

help:
	@echo ""
	@echo "FinLake — Financial Market Data Lakehouse"
	@echo "==========================================="
	@echo "  make docker-up        Start all Docker services (Kafka, MinIO, Airflow, Spark)"
	@echo "  make docker-down      Stop all Docker services"
	@echo "  make ingest-batch     Run batch OHLCV + company metadata ingestion"
	@echo "  make ingest-stream    Start Kafka tick producer (streaming simulation)"
	@echo "  make transform-silver Run Bronze → Silver PySpark job"
	@echo "  make transform-gold   Run Silver → Gold PySpark job"
	@echo "  make run-dq           Run data quality checks"
	@echo "  make reconcile        Run reconciliation framework"
	@echo "  make test             Run all unit tests"
	@echo "  make lint             Run ruff linter"
	@echo ""

docker-up:
	docker compose -f docker/docker-compose.yml up -d
	@echo "Waiting for services to start..."
	sleep 15
	@echo "Services ready. MinIO console: http://localhost:9001 | Airflow: http://localhost:8080"

docker-down:
	docker compose -f docker/docker-compose.yml down -v

ingest-batch:
	python -m src.ingestion.batch_ingestor
	python -m src.ingestion.company_ingestor

ingest-stream:
	python -m src.ingestion.tick_producer

transform-silver:
	python -m src.transformation.bronze_to_silver
	python -m src.transformation.scd2_company_dim

transform-gold:
	python -m src.transformation.silver_to_gold

run-dq:
	python -m src.quality.dq_checks

reconcile:
	python -m src.quality.reconciliation

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ dags/
