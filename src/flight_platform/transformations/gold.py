"""Gold layer: small analytics tables built from Silver, ready for the warehouse and Power BI.

Tables (each partitioned by date under data/gold/<table>/):
  hourly_traffic   per date + hour (UTC): aircraft, positions, average speed and altitude
  country_traffic  per date + origin country: the same metrics, plus activity_rank (1 = most active)

Averages only use aircraft that are in the air (on-ground rows would pull them down).
`origin_country` is the country where the aircraft is registered, not where it is flying.
Reprocessing a date replaces that date's Gold data, so it is safe to re-run.

Run:  python -m flight_platform.transformations.gold [--date YYYY-MM-DD]   (default: today, UTC)
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from flight_platform.config import settings
from flight_platform.log_config import get_logger
from flight_platform.spark_session import get_spark
from flight_platform.transformations.lake import read_date, write_date_partitions

logger = get_logger(__name__)

MS_TO_KMH = 3.6


def _traffic_metrics() -> list[Column]:
    """The metrics shared by every Gold table."""
    in_air = ~F.coalesce(F.col("on_ground"), F.lit(False))
    return [
        F.countDistinct("icao24").alias("aircraft_count"),
        F.count("*").alias("position_count"),
        F.round(F.avg(F.when(in_air, F.col("velocity") * MS_TO_KMH)), 1).alias("avg_velocity_kmh"),
        F.round(F.avg(F.when(in_air, F.col("baro_altitude"))), 1).alias("avg_altitude_m"),
    ]


def hourly_traffic(silver: DataFrame) -> DataFrame:
    return (
        silver.withColumn("hour", F.hour("ingested_at"))
        .groupBy("date", "hour")
        .agg(*_traffic_metrics())
        .orderBy("date", "hour")
    )


def country_traffic(silver: DataFrame) -> DataFrame:
    rank_window = Window.partitionBy("date").orderBy(
        F.desc("aircraft_count"), F.desc("position_count"), "origin_country"
    )
    return (
        silver.withColumn("origin_country", F.coalesce("origin_country", F.lit("Unknown")))
        .groupBy("date", "origin_country")
        .agg(*_traffic_metrics())
        .withColumn("activity_rank", F.rank().over(rank_window))
        .orderBy("date", "activity_rank")
    )


def run_gold(
    date: str | None = None,
    spark: SparkSession | None = None,
    silver_dir: Path | None = None,
    gold_dir: Path | None = None,
) -> dict[str, int]:
    """Build every Gold table for one Silver date. Returns {table_name: row_count}."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    silver_dir = silver_dir or settings.silver_dir
    gold_dir = gold_dir or settings.gold_dir

    spark = spark or get_spark("gold")
    silver = read_date(spark, silver_dir, date)
    if silver is None:
        logger.warning("No Silver data for %s in %s; nothing to do", date, silver_dir)
        return {"hourly_traffic": 0, "country_traffic": 0}

    silver = silver.cache()
    tables = {"hourly_traffic": hourly_traffic(silver), "country_traffic": country_traffic(silver)}
    counts = {}
    for name, df in tables.items():
        counts[name] = df.count()
        if counts[name]:
            write_date_partitions(df, gold_dir / name)
    silver.unpersist()
    logger.info("Gold %s: %s", date, counts)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Gold tables from Silver for one date")
    parser.add_argument("--date", help="Silver date to process, YYYY-MM-DD (default: today UTC)")
    run_gold(parser.parse_args().date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
