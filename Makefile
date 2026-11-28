.PHONY: install test lint format

install:
	pip install -e ".[dev,spark]"

test:
	pytest

lint:
	ruff check src tests

format:
	ruff check --fix src tests
	ruff format src tests

ingest:
	python -m flight_platform.ingestion.main --limit 5

bronze:
	python -m flight_platform.transformations.bronze	
	
silver:
	python -m flight_platform.transformations.silver
	
gold:
	python -m flight_platform.transformations.gold

db-up:
	docker compose up -d --wait sqlserver

db-down:
	docker compose down

warehouse-init:
	python -m flight_platform.warehouse.sql_server_loader init

load:
	python -m flight_platform.warehouse.sql_server_loader load
	