# Flight Data Platform

Batch data engineering platform for flight traffic analytics.

```
OpenSky API -> Bronze (raw Parquet) -> Silver (clean) -> Gold (analytics)
            -> SQL Server Data Warehouse -> Power BI
Airflow orchestrates the scheduled pipeline. Docker runs the services.
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
cp .env.example .env             # then edit the values
make install
make test
```

## Run the pipeline

```bash
make ingest          # OpenSky -> console (check the API works)
make bronze          # OpenSky -> data/bronze
make silver          # Bronze -> clean + quality checks -> data/silver, data/quarantine
make gold            # Silver -> analytics tables -> data/gold
make db-up           # start SQL Server in Docker (needs Docker + SQLSERVER_PASSWORD in .env)
make warehouse-init  # create database, schemas, tables (once)
make load            # Gold -> SQL Server (Power BI then reads the `bi.*` views)
```

Spark needs Java 17 or newer (`java -version`). On Windows, use WSL2.

## Status

- [x] Phase 0 - project skeleton, config, logging
- [x] Phase 1 - OpenSky ingestion (`make ingest`)
- [x] Phase 2 - Bronze (`make bronze`, needs Java 17+)
- [x] Phase 3 - Silver + data quality (`make silver`)
- [x] Phase 4 - Gold (`make gold`)
- [x] Phase 5 - SQL Server warehouse (`make db-up`, `make warehouse-init`, `make load`)
- [ ] Phase 6 - Power BI (views are ready; build the dashboard with `docs/powerbi-guide.md`)
- [ ] Phase 7 - Airflow
- [ ] Phase 8 - Docker, docs, CI
