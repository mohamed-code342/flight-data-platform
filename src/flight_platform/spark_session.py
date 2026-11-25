"""One place to create the SparkSession, so every job uses the same settings."""

from pyspark.sql import SparkSession

from flight_platform.config import settings


def get_spark(app_name: str = "flight-platform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
