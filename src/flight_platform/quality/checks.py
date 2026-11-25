"""Data quality rules. To add or change a rule, edit `quality_rules()` only.

A rule is a condition that must be TRUE for a good row. Rows that break any rule are
separated from the good ones and keep the list of broken rules in `reject_reasons`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def quality_rules() -> dict[str, Column]:
    """rule name -> condition a valid row satisfies (a null result counts as a failure)."""
    velocity = F.col("velocity")
    altitude = F.col("baro_altitude")
    return {
        "icao24_invalid": F.col("icao24").rlike("^[0-9a-f]{6}$"),
        "last_contact_missing": F.col("last_contact_time").isNotNull(),
        # A missing position counts as invalid: a row without lat/lon is useless for our analysis
        "latitude_invalid": F.col("latitude").between(-90, 90),
        "longitude_invalid": F.col("longitude").between(-180, 180),
        # Optional fields: null is fine, but if present the value must make sense
        "velocity_negative": velocity.isNull() | (velocity >= 0),
        "altitude_out_of_range": altitude.isNull() | altitude.between(-1000, 30000),
    }


def apply_quality_checks(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, invalid). `invalid` has an extra `reject_reasons` array column."""
    failed = F.array(
        *[
            F.when(~F.coalesce(condition, F.lit(False)), F.lit(name))
            for name, condition in quality_rules().items()
        ]
    )
    checked = df.withColumn("reject_reasons", F.filter(failed, lambda reason: reason.isNotNull()))
    valid = checked.filter(F.size("reject_reasons") == 0).drop("reject_reasons")
    invalid = checked.filter(F.size("reject_reasons") > 0)
    return valid, invalid
