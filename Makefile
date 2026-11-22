.PHONY: install test lint format

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check src tests

format:
	ruff check --fix src tests
	ruff format src tests
