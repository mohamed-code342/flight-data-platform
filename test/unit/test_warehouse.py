from datetime import date

import pytest

from flight_platform.transformations.lake import write_date_partitions
from flight_platform.warehouse.sql_server_loader import init_warehouse, load_gold

HOURLY_SCHEMA = (
    "date string, hour int, aircraft_count long, position_count long, "
    "avg_velocity_kmh double, avg_altitude_m double"
)
COUNTRY_SCHEMA = (
    "date string, origin_country string, aircraft_count long, position_count long, "
    "avg_velocity_kmh double, avg_altitude_m double, activity_rank int"
)


class FakeCursor:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on = log, fail_on

    def execute(self, sql, params=None):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        self.log.append(("execute", sql))

    def executemany(self, sql, rows):
        self.log.append(("executemany", sql, list(rows)))


class FakeConnection:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on = log, fail_on

    def cursor(self):
        return FakeCursor(self.log, self.fail_on)

    def commit(self):
        self.log.append(("commit",))

    def rollback(self):
        self.log.append(("rollback",))

    def close(self):
        self.log.append(("close",))


class FakeServer:
    """Stands in for get_connection and records everything that happens."""

    def __init__(self, fail_on=None):
        self.log, self.fail_on, self.connects = [], fail_on, []

    def __call__(self, database=None, autocommit=False):
        self.connects.append((database, autocommit))
        return FakeConnection(self.log, self.fail_on)

    def sql(self):
        return [e[1] for e in self.log if e[0] == "execute"]

    def index_of(self, text):
        return next(i for i, sql in enumerate(self.sql()) if text in sql)


def write_gold(spark, gold_dir):
    hourly = spark.createDataFrame([("2026-11-23", 14, 2, 3, 540.0, 11000.0)], HOURLY_SCHEMA)
    country = spark.createDataFrame(
        [("2026-11-23", "Germany", 2, 3, 540.0, 11000.0, 1),
         ("2026-11-23", "Egypt", 1, 1, None, None, 2)],
        COUNTRY_SCHEMA,
    )  # fmt: skip
    write_date_partitions(hourly, gold_dir / "hourly_traffic")
    write_date_partitions(country, gold_dir / "country_traffic")


def test_init_creates_database_then_tables_in_order():
    server = FakeServer()
    init_warehouse("flight_dw", connect=server)

    # CREATE DATABASE cannot run in a transaction, so the first connection is autocommit
    assert server.connects == [("master", True), ("flight_dw", False)]
    assert "CREATE DATABASE [flight_dw]" in server.sql()[0]
    assert server.index_of("CREATE SCHEMA dw") < server.index_of("CREATE TABLE dw.dim_date")
    assert server.index_of("CREATE TABLE dw.dim_country") < server.index_of(
        "CREATE TABLE dw.fact_country_traffic"
    )  # the fact references the dimension, so the dimension comes first
    assert server.index_of("CREATE TABLE dw.fact_hourly_traffic") < server.index_of(
        "CREATE TABLE stg.hourly_traffic"
    )
    for view in ("vw_hourly_traffic", "vw_country_traffic", "vw_top_countries", "vw_daily_summary"):
        assert server.index_of(f"CREATE OR ALTER VIEW bi.{view}") > server.index_of(
            "CREATE TABLE stg.country_traffic"
        )  # views come last, after every table they read
    assert ("commit",) in server.log


@pytest.mark.parametrize("name", ["bad;name", "x]DROP", "with space"])
def test_init_rejects_unsafe_database_names(name):
    with pytest.raises(ValueError):
        init_warehouse(name, connect=FakeServer())


def test_load_fills_staging_then_dimensions_then_facts(spark, tmp_path):
    write_gold(spark, tmp_path)
    server = FakeServer()
    counts = load_gold("2026-11-23", spark, tmp_path, connect=server)

    assert counts == {"hourly_traffic": 1, "country_traffic": 2}

    # staging rows: dates are real date objects, nulls stay None
    inserts = {e[1].split("(")[0].strip(): e[2] for e in server.log if e[0] == "executemany"}
    assert inserts["INSERT INTO stg.hourly_traffic"] == [
        (date(2026, 11, 23), 14, 2, 3, 540.0, 11000.0)
    ]
    countries = {row[1]: row for row in inserts["INSERT INTO stg.country_traffic"]}
    assert countries["Egypt"] == (date(2026, 11, 23), "Egypt", 1, 1, None, None, 2)

    # order: empty staging -> dimensions -> facts -> commit
    first_sql = server.sql()
    assert first_sql[0].startswith("TRUNCATE TABLE stg.hourly_traffic")
    assert server.index_of("INSERT INTO dw.dim_date") < server.index_of("INSERT INTO dw.fact_")
    assert server.index_of("INSERT INTO dw.dim_country") < server.index_of(
        "INSERT INTO dw.fact_country_traffic"
    )
    assert server.log[-2:] == [("commit",), ("close",)]
    assert ("rollback",) not in server.log


def test_load_rolls_back_everything_on_error(spark, tmp_path):
    write_gold(spark, tmp_path)
    server = FakeServer(fail_on="INSERT INTO dw.fact_country_traffic")
    with pytest.raises(RuntimeError):
        load_gold("2026-11-23", spark, tmp_path, connect=server)
    assert ("rollback",) in server.log
    assert ("commit",) not in server.log
    assert server.log[-1] == ("close",)


def test_load_without_gold_data_does_not_connect(spark, tmp_path):
    server = FakeServer()
    counts = load_gold("2026-01-01", spark, tmp_path, connect=server)
    assert counts == {"hourly_traffic": 0, "country_traffic": 0}
    assert server.connects == []


def test_sql_files_are_sendable_as_single_batches():
    from flight_platform.config import settings

    for path in settings.sql_dir.rglob("*.sql"):
        text = path.read_text(encoding="utf-8")
        lines = [line.strip().upper() for line in text.splitlines()]
        assert "GO" not in lines, f"{path.name}: GO is not supported, one batch per file"


def test_each_view_file_is_one_create_view_statement():
    from flight_platform.config import settings

    files = sorted((settings.sql_dir / "views").glob("*.sql"))
    assert len(files) == 4
    for path in files:
        code = "\n".join(
            line for line in path.read_text(encoding="utf-8").splitlines()
            if not line.strip().startswith("--")
        ).strip()  # fmt: skip
        assert code.upper().startswith("CREATE OR ALTER VIEW BI."), path.name
        assert code.upper().count("CREATE ") == 1, f"{path.name}: one view per file"
