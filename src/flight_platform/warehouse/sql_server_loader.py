"""Load the Gold tables into the SQL Server warehouse.

Steps of a load (all in ONE transaction, so a failure leaves the warehouse untouched):
  1. empty the staging tables and fill them with the Gold rows of one date
  2. run sql/dimensions/*.sql  (add new dates and countries)
  3. run sql/facts/*.sql       (replace that date's rows in the fact tables)

Gold tables are small aggregates (hundreds of rows per day), so rows are collected
to the driver and inserted with pymssql. No JDBC driver or extra Spark setup is needed.

Run:  python -m flight_platform.warehouse.sql_server_loader init
      python -m flight_platform.warehouse.sql_server_loader load [--date YYYY-MM-DD]
"""

import argparse
import re
from collections.abc import Callable
from datetime import date as Date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymssql
from pyspark.sql import SparkSession

from flight_platform.config import settings
from flight_platform.log_config import get_logger
from flight_platform.spark_session import get_spark
from flight_platform.transformations.lake import read_date
from flight_platform.warehouse.connections import get_connection

logger = get_logger(__name__)

Connect = Callable[..., Any]

INSERT_HOURLY = (
    "INSERT INTO stg.hourly_traffic (load_date, hour_of_day, aircraft_count, position_count, "
    "avg_velocity_kmh, avg_altitude_m) VALUES (%s, %s, %s, %s, %s, %s)"
)
INSERT_COUNTRY = (
    "INSERT INTO stg.country_traffic (load_date, origin_country, aircraft_count, position_count, "
    "avg_velocity_kmh, avg_altitude_m, activity_rank) VALUES (%s, %s, %s, %s, %s, %s, %s)"
)


def run_sql_files(cursor, folder: Path) -> None:
    """Execute every .sql file of a folder in alphabetical order (one batch per file)."""
    for path in sorted(folder.glob("*.sql")):
        logger.info("Running %s/%s", folder.name, path.name)
        cursor.execute(path.read_text(encoding="utf-8"))


def init_warehouse(database: str | None = None, connect: Connect = get_connection) -> None:
    """Create the database, schemas, tables and Power BI views. Safe to run again."""
    database = database or settings.sqlserver_database
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise ValueError(f"Database name may only contain letters, digits and _: {database!r}")

    # CREATE DATABASE cannot run inside a transaction, hence autocommit
    conn = connect("master", autocommit=True)
    try:
        conn.cursor().execute(f"IF DB_ID(N'{database}') IS NULL CREATE DATABASE [{database}]")
    finally:
        conn.close()

    conn = connect(database)
    try:
        cursor = conn.cursor()
        run_sql_files(cursor, settings.sql_dir / "ddl")
        run_sql_files(cursor, settings.sql_dir / "staging")
        run_sql_files(cursor, settings.sql_dir / "views")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("Warehouse %s is ready", database)


def _as_date(value: Date | str) -> Date:
    """Spark infers the `date=YYYY-MM-DD` partition folder as a date; accept a string too."""
    return Date.fromisoformat(value) if isinstance(value, str) else value


def _hourly_rows(df) -> list[tuple]:
    return [
        (_as_date(r["date"]), r["hour"], r["aircraft_count"], r["position_count"],
         r["avg_velocity_kmh"], r["avg_altitude_m"])
        for r in df.collect()
    ]  # fmt: skip


def _country_rows(df) -> list[tuple]:
    return [
        (_as_date(r["date"]), r["origin_country"], r["aircraft_count"],
         r["position_count"], r["avg_velocity_kmh"], r["avg_altitude_m"], r["activity_rank"])
        for r in df.collect()
    ]  # fmt: skip


def load_gold(
    date: str | None = None,
    spark: SparkSession | None = None,
    gold_dir: Path | None = None,
    connect: Connect = get_connection,
) -> dict[str, int]:
    """Load one date of Gold into the warehouse. Returns {table_name: rows_loaded}."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    gold_dir = gold_dir or settings.gold_dir

    spark = spark or get_spark("warehouse-load")
    hourly = read_date(spark, gold_dir / "hourly_traffic", date)
    country = read_date(spark, gold_dir / "country_traffic", date)
    if hourly is None and country is None:
        logger.warning("No Gold data for %s in %s; nothing to load", date, gold_dir)
        return {"hourly_traffic": 0, "country_traffic": 0}

    hourly_rows = _hourly_rows(hourly) if hourly is not None else []
    country_rows = _country_rows(country) if country is not None else []

    conn = connect(settings.sqlserver_database)
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE stg.hourly_traffic")
        cursor.execute("TRUNCATE TABLE stg.country_traffic")
        if hourly_rows:
            cursor.executemany(INSERT_HOURLY, hourly_rows)
        if country_rows:
            cursor.executemany(INSERT_COUNTRY, country_rows)
        run_sql_files(cursor, settings.sql_dir / "dimensions")
        run_sql_files(cursor, settings.sql_dir / "facts")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    counts = {"hourly_traffic": len(hourly_rows), "country_traffic": len(country_rows)}
    logger.info("Loaded Gold %s into SQL Server: %s", date, counts)
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SQL Server warehouse tasks")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="create the database, schemas and tables (safe to re-run)")
    load = commands.add_parser("load", help="load one date of Gold into the warehouse")
    load.add_argument("--date", help="YYYY-MM-DD (default: today UTC)")
    args = parser.parse_args(argv)

    try:
        if args.command == "init":
            init_warehouse()
        else:
            load_gold(args.date)
    except pymssql.Error as exc:
        logger.error("SQL Server error: %s. Is it running? Try: make db-up", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
