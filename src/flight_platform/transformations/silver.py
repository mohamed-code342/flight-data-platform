"""Silver layer: cleaned, validated, de-duplicated flight states.

Bronze (one date) -> clean -> quality checks -> remove duplicates -> data/silver
                                              \\-> bad rows -> data/quarantine

Reprocessing a date replaces that date's Silver and quarantine data, so it is safe to re-run.

Run:  python -m flight_platform.transformations.silver [--date YYYY-MM-DD]   (default: today, UTC)
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from flight_platform.config import settings
from flight_platform.log_config import get_logger
from flight_platform.quality.checks import apply_quality_checks
from flight_platform.spark_session import get_spark
from flight_platform.transformations.lake import read_date, write_date_partitions

logger = get_logger(__name__)

SILVER_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "position_time",
    "last_contact_time",
    "longitude",
    "latitude",
    "baro_altitude",
    "geo_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "squawk",
    "spi",
    "position_source",
    "category",
    "snapshot_time",
    "ingested_at",
    "source",
    "date",
]


def clean(df: DataFrame) -> DataFrame:
    """Standardise text, convert epoch seconds to timestamps, keep only the Silver columns."""
    callsign = F.trim("callsign")
    return (
        df.withColumn("icao24", F.lower(F.trim("icao24")))
        .withColumn("callsign", F.when(callsign == "", None).otherwise(callsign))
        .withColumn("origin_country", F.trim("origin_country"))
        .withColumn("position_time", F.timestamp_seconds("time_position"))
        .withColumn("last_contact_time", F.timestamp_seconds("last_contact"))
        .withColumn("snapshot_time", F.timestamp_seconds("snapshot_time"))
        .select(*SILVER_COLUMNS)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per (icao24, last_contact_time): the first time we received it.

    This removes both re-runs of the same snapshot and records that did not change
    between two consecutive snapshots.
    """
    window = Window.partitionBy("icao24", "last_contact_time").orderBy("ingested_at")
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def run_silver(
    date: str | None = None,
    spark: SparkSession | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
    quarantine_dir: Path | None = None,
) -> dict[str, int]:
    """Process one Bronze date. Returns {"valid": n, "rejected": m}."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bronze_dir = bronze_dir or settings.bronze_dir
    silver_dir = silver_dir or settings.silver_dir
    quarantine_dir = quarantine_dir or settings.quarantine_dir

    spark = spark or get_spark("silver")
    bronze = read_date(spark, bronze_dir, date)
    if bronze is None:
        logger.warning("No Bronze data for %s in %s; nothing to do", date, bronze_dir)
        return {"valid": 0, "rejected": 0}

    valid, invalid = apply_quality_checks(clean(bronze))
    silver = deduplicate(valid).cache()
    rejected = invalid.cache()
    counts = {"valid": silver.count(), "rejected": rejected.count()}

    if counts["valid"]:
        write_date_partitions(silver, silver_dir)
    if counts["rejected"]:
        write_date_partitions(rejected, quarantine_dir)

    silver.unpersist()
    rejected.unpersist()
    logger.info(
        "Silver %s: %d valid rows, %d rows to quarantine", date, counts["valid"], counts["rejected"]
    )
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Silver from Bronze for one date")
    parser.add_argument("--date", help="Bronze date to process, YYYY-MM-DD (default: today UTC)")
    run_silver(parser.parse_args().date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
