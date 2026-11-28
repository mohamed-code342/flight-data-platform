"""Tiny helpers for the date-partitioned Parquet folders of the Data Lake."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def read_date(spark: SparkSession, base_dir: Path, date: str) -> DataFrame | None:
    """Read one `date=YYYY-MM-DD` partition (with the `date` column). None if it does not exist."""
    partition = base_dir / f"date={date}"
    if not partition.exists():
        return None
    return spark.read.option("basePath", str(base_dir)).parquet(str(partition))


def write_date_partitions(df: DataFrame, path: Path) -> None:
    """Replace only the date partitions present in df and leave the other dates untouched."""
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("date")
        .parquet(str(path))
    )
