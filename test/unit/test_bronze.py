from datetime import datetime, timezone

from flight_platform.ingestion.opensky_client import STATE_FIELDS
from flight_platform.transformations.bronze import build_bronze_df, write_bronze

INGESTED_AT = datetime(2026, 11, 23, 14, 5, tzinfo=timezone.utc)


def make_snapshot(*states):
    return {"time": 1700000001, "states": list(states)}


def test_adds_metadata_and_keeps_api_columns(spark, make_state):
    df = build_bronze_df(spark, make_snapshot(make_state()), INGESTED_AT)
    assert df.columns == [*STATE_FIELDS, "snapshot_time", "ingested_at", "source", "date", "hour"]
    row = df.first()
    assert row["source"] == "opensky"
    assert row["snapshot_time"] == 1700000001
    assert (row["date"], row["hour"]) == ("2026-11-23", "14")


def test_whole_numbers_are_accepted_for_double_columns(spark, make_state):
    # JSON delivers 8 and 0 as ints; Spark double columns must still accept them
    state = make_state(longitude=8, velocity=0)
    df = build_bronze_df(spark, make_snapshot(state), INGESTED_AT)
    row = df.first()
    assert row["longitude"] == 8.0 and row["velocity"] == 0.0
    assert dict(df.dtypes)["longitude"] == "double"


def test_missing_values_stay_null(spark, make_state):
    state = make_state(latitude=None, longitude=None, callsign=None)
    row = build_bronze_df(spark, make_snapshot(state), INGESTED_AT).first()
    assert row["latitude"] is None and row["callsign"] is None


def test_write_creates_date_hour_partitions_and_appends(spark, make_state, tmp_path):
    snapshot = make_snapshot(make_state(), make_state(icao24="abc123"))
    df = build_bronze_df(spark, snapshot, INGESTED_AT)

    assert write_bronze(df, tmp_path) == 2
    assert (tmp_path / "date=2026-11-23" / "hour=14").is_dir()

    write_bronze(df, tmp_path)  # a second run adds files, it does not overwrite
    assert spark.read.parquet(str(tmp_path)).count() == 4


def test_empty_snapshot_writes_nothing(spark, tmp_path):
    df = build_bronze_df(spark, make_snapshot(), INGESTED_AT)
    assert write_bronze(df, tmp_path) == 0
    assert list(tmp_path.iterdir()) == []
