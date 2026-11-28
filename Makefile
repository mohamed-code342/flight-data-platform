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
	