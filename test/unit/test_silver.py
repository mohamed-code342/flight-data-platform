from datetime import datetime, timezone

from flight_platform.transformations.bronze import build_bronze_df, write_bronze
from flight_platform.transformations.silver import clean, deduplicate, run_silver


def snapshot(*states):
    return {"time": 1700000001, "states": list(states)}


def at(hour):
    return datetime(2026, 11, 23, hour, 0, tzinfo=timezone.utc)


def test_clean_standardises_text_and_converts_times(spark, make_state):
    bronze = build_bronze_df(
        spark, snapshot(make_state(icao24=" 3C6444 ", callsign="  DLH123  ")), at(14)
    )
    row = clean(bronze).first()
    assert row["icao24"] == "3c6444"
    assert row["callsign"] == "DLH123"
    assert row["last_contact_time"] == datetime(2023, 11, 14, 22, 13, 21)  # 1700000001 (UTC)
    assert "sensors" not in clean(bronze).columns and "time_position" not in clean(bronze).columns


def test_blank_callsign_becomes_null(spark, make_state):
    bronze = build_bronze_df(spark, snapshot(make_state(callsign="        ")), at(14))
    assert clean(bronze).first()["callsign"] is None


def test_deduplicate_keeps_first_received_record(spark, make_state):
    first = build_bronze_df(spark, snapshot(make_state()), at(14))
    again = build_bronze_df(spark, snapshot(make_state()), at(15))  # same record, later snapshot
    moved = build_bronze_df(spark, snapshot(make_state(last_contact=1700000099)), at(15))
    rows = deduplicate(clean(first.unionByName(again).unionByName(moved))).collect()
    assert len(rows) == 2
    original = next(r for r in rows if r["last_contact_time"].second == 21)
    assert original["ingested_at"] == datetime(2026, 11, 23, 14, 0)


def test_run_silver_end_to_end_and_rerun_is_safe(spark, make_state, tmp_path):
    bronze, silver, quarantine = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    good, bad = make_state(), make_state(icao24="ffffff", latitude=95.0)
    for hour in (14, 15):  # the same two records show up in two snapshots
        write_bronze(build_bronze_df(spark, snapshot(good, bad), at(hour)), bronze)

    for _ in range(2):  # running twice must give the same result
        counts = run_silver("2026-11-23", spark, bronze, silver, quarantine)
        assert counts == {"valid": 1, "rejected": 2}
        assert spark.read.parquet(str(silver)).count() == 1
        rejected = spark.read.parquet(str(quarantine))
        assert rejected.count() == 2
        assert all(r["reject_reasons"] == ["latitude_invalid"] for r in rejected.collect())

    assert (silver / "date=2026-11-23").is_dir()


def test_run_silver_without_bronze_data_does_nothing(spark, tmp_path):
    counts = run_silver("2026-01-01", spark, tmp_path / "bronze", tmp_path / "s", tmp_path / "q")
    assert counts == {"valid": 0, "rejected": 0}
    assert not (tmp_path / "s").exists()
