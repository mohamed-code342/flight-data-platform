from datetime import datetime, timezone

from flight_platform.transformations.bronze import build_bronze_df, write_bronze
from flight_platform.transformations.gold import country_traffic, hourly_traffic, run_gold
from flight_platform.transformations.silver import clean, run_silver


def at(hour):
    return datetime(2026, 11, 23, hour, 30, tzinfo=timezone.utc)


def bronze(spark, hour, *states):
    return build_bronze_df(spark, {"time": 1700000001, "states": list(states)}, at(hour))


def silver_like(spark, *per_hour):
    """per_hour: (hour, [states]) -> a Silver-shaped DataFrame."""
    dfs = [bronze(spark, hour, *states) for hour, states in per_hour]
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df)
    return clean(combined)


def test_hourly_traffic_metrics(spark, make_state):
    silver = silver_like(
        spark,
        (14, [
            make_state(icao24="aaaaaa", velocity=100.0, baro_altitude=10000.0),
            make_state(icao24="aaaaaa", velocity=200.0, baro_altitude=12000.0,
                       last_contact=1700000050),
            make_state(icao24="bbbbbb", on_ground=True, velocity=0.0, baro_altitude=0.0),
        ]),
        (15, [make_state(icao24="cccccc", velocity=50.0, baro_altitude=8000.0)]),
    )  # fmt: skip
    rows = {r["hour"]: r for r in hourly_traffic(silver).collect()}

    h14 = rows[14]
    assert h14["aircraft_count"] == 2  # aaaaaa counted once even with two positions
    assert h14["position_count"] == 3
    assert h14["avg_velocity_kmh"] == 540.0  # (360 + 720) / 2, the grounded plane is excluded
    assert h14["avg_altitude_m"] == 11000.0
    assert rows[15]["avg_velocity_kmh"] == 180.0


def test_country_traffic_ranks_by_activity(spark, make_state):
    silver = silver_like(
        spark,
        (14, [
            make_state(icao24="aaaaaa", origin_country="Egypt"),
            make_state(icao24="bbbbbb", origin_country="Germany"),
            make_state(icao24="cccccc", origin_country="Germany"),
            make_state(icao24="dddddd", origin_country=None),
        ]),
    )  # fmt: skip
    rows = country_traffic(silver).collect()
    assert [(r["origin_country"], r["activity_rank"]) for r in rows][0] == ("Germany", 1)
    assert rows[0]["aircraft_count"] == 2
    assert {r["origin_country"] for r in rows} == {"Germany", "Egypt", "Unknown"}


def test_run_gold_end_to_end_and_rerun_is_safe(spark, make_state, tmp_path):
    bronze_dir, silver_dir, gold_dir = tmp_path / "b", tmp_path / "s", tmp_path / "g"
    write_bronze(bronze(spark, 14, make_state(icao24="aaaaaa")), bronze_dir)
    second = make_state(icao24="bbbbbb", last_contact=1700000077)
    write_bronze(bronze(spark, 15, second), bronze_dir)
    run_silver("2026-11-23", spark, bronze_dir, silver_dir, tmp_path / "q")

    for _ in range(2):  # running twice must not double the rows
        counts = run_gold("2026-11-23", spark, silver_dir, gold_dir)
        assert counts == {"hourly_traffic": 2, "country_traffic": 1}
        assert spark.read.parquet(str(gold_dir / "hourly_traffic")).count() == 2
        assert spark.read.parquet(str(gold_dir / "country_traffic")).count() == 1

    assert (gold_dir / "country_traffic" / "date=2026-11-23").is_dir()


def test_run_gold_without_silver_data_does_nothing(spark, tmp_path):
    counts = run_gold("2026-01-01", spark, tmp_path / "s", tmp_path / "g")
    assert counts == {"hourly_traffic": 0, "country_traffic": 0}
    assert not (tmp_path / "g").exists()
