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

Spark needs Java 17 or newer (`java -version`). On Windows, use WSL2.

## Status

- [x] Phase 0 - project skeleton, config, logging
- [x] Phase 1 - OpenSky ingestion (`make ingest`)
- [x] Phase 2 - Bronze (`make bronze`, needs Java 17+)
- [x] Phase 3 - Silver + data quality
- [ ] Phase 4 - Gold
- [ ] Phase 5 - SQL Server warehouse
- [ ] Phase 6 - Power BI
- [ ] Phase 7 - Airflow
- [ ] Phase 8 - Docker, docs, CI
