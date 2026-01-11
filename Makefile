.PHONY: help build up down logs ingest dbt-run dbt-test dbt-docs clean

help:
	@echo "Available commands:"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start all services"
	@echo "  make down       - Stop all services"
	@echo "  make logs       - View logs"
	@echo "  make ingest     - Run data ingestion"
	@echo "  make dbt-run    - Run dbt models"
	@echo "  make dbt-test   - Run dbt tests"
	@echo "  make dbt-docs   - Generate dbt docs"
	@echo "  make pipeline   - Run full pipeline"
	@echo "  make clean      - Remove containers and volumes"

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

ingest:
	docker-compose exec ingestion python -m ingestion.main

dbt-run:
	docker-compose exec dbt dbt run

dbt-test:
	docker-compose exec dbt dbt test

dbt-docs:
	docker-compose exec dbt dbt docs generate

pipeline: up
	@echo "Waiting for services to start..."
	@sleep 5
	@echo "Running ingestion..."
	docker-compose exec ingestion python -m ingestion.main
	@echo "Running dbt transformations..."
	docker-compose exec dbt dbt run
	@echo "Running dbt tests..."
	docker-compose exec dbt dbt test
	@echo "Pipeline complete!"

clean:
	docker-compose down -v
	rm -rf dbt_project/target dbt_project/logs
