"""Bronze layer: the raw OpenSky snapshot, stored as it came, plus a little metadata.

No cleaning here. Silver does that. Bronze only guarantees: explicit column types,
when we received the data (ingested_at), and where it came from (source).

Run:  python -m flight_platform.transformations.bronze
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from flight_platform.config import settings
from flight_platform.ingestion.opensky_client import STATE_FIELDS, OpenSkyClient, OpenSkyError
from flight_platform.log_config import get_logger
from flight_platform.spark_session import get_spark

logger = get_logger(__name__)

# Types of the columns that come from the API (same order as STATE_FIELDS)
API_SCHEMA = StructType(
    [
        StructField("icao24", StringType()),
        StructField("callsign", StringType()),
        StructField("origin_country", StringType()),
        StructField("time_position", LongType()),
        StructField("last_contact", LongType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("baro_altitude", DoubleType()),
        StructField("on_ground", BooleanType()),
        StructField("velocity", DoubleType()),
        StructField("true_track", DoubleType()),
        StructField("vertical_rate", DoubleType()),
        StructField("sensors", ArrayType(IntegerType())),
        StructField("geo_altitude", DoubleType()),
        StructField("squawk", StringType()),
        StructField("spi", BooleanType()),
        StructField("position_source", IntegerType()),
        StructField("category", IntegerType()),
    ]
)
assert tuple(f.name for f in API_SCHEMA) == STATE_FIELDS

_DOUBLE_FIELDS = {f.name for f in API_SCHEMA if isinstance(f.dataType, DoubleType)}


def _to_row(state: dict[str, Any]) -> tuple:
    """dict -> tuple in schema order. JSON gives ints like 0, but Spark wants floats for doubles."""
    values = []
    for name in STATE_FIELDS:
        value = state.get(name)
        if value is not None and name in _DOUBLE_FIELDS:
            value = float(value)
        values.append(value)
    return tuple(values)


def build_bronze_df(
    spark: SparkSession, snapshot: dict[str, Any], ingested_at: datetime | None = None
) -> DataFrame:
    """Turn one snapshot ({"time", "states"}) into the Bronze DataFrame."""
    ingested_at = ingested_at or datetime.now(timezone.utc)
    rows = [_to_row(state) for state in snapshot["states"]]
    df = spark.createDataFrame(rows, schema=API_SCHEMA)
    return (
        df.withColumn("snapshot_time", F.lit(snapshot.get("time")).cast("long"))
        .withColumn("ingested_at", F.lit(ingested_at))
        .withColumn("source", F.lit("opensky"))
        .withColumn("date", F.lit(ingested_at.strftime("%Y-%m-%d")))
        .withColumn("hour", F.lit(ingested_at.strftime("%H")))
    )


def write_bronze(df: DataFrame, bronze_dir: Path) -> int:
    """Append to data/bronze/date=YYYY-MM-DD/hour=HH/. Returns the number of rows written."""
    count = df.count()
    if count == 0:
        logger.warning("Snapshot has no aircraft; nothing written")
        return 0
    df.write.mode("append").partitionBy("date", "hour").parquet(str(bronze_dir))
    logger.info("Wrote %d rows to %s", count, bronze_dir)
    return count


def run_bronze(snapshot: dict[str, Any] | None = None, spark: SparkSession | None = None) -> int:
    """Fetch (unless a snapshot is given), build Bronze, write it."""
    snapshot = snapshot or OpenSkyClient().get_states()
    spark = spark or get_spark("bronze")
    return write_bronze(build_bronze_df(spark, snapshot), settings.bronze_dir)


def main() -> int:
    try:
        run_bronze()
    except OpenSkyError as exc:
        logger.error("Bronze failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
