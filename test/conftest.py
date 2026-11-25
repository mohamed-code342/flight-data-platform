import shutil

import pytest

from flight_platform.ingestion.opensky_client import STATE_FIELDS


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Java is required to run Spark tests")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def make_state():
    """Build one OpenSky state dict. A valid aircraft by default; override fields per test."""

    def _make(**overrides):
        state = dict.fromkeys(STATE_FIELDS)
        state.update(
            icao24="3c6444", callsign="DLH123 ", origin_country="Germany",
            time_position=1700000000, last_contact=1700000001, longitude=8.5, latitude=50.5,
            baro_altitude=10000, velocity=230, on_ground=False,
        )  # fmt: skip
        state.update(overrides)
        return state

    return _make
