import pytest

from flight_platform.quality.checks import apply_quality_checks
from flight_platform.transformations.bronze import build_bronze_df
from flight_platform.transformations.silver import clean


def checked(spark, states):
    bronze = build_bronze_df(spark, {"time": 1700000001, "states": states})
    return apply_quality_checks(clean(bronze))


def test_good_row_passes(spark, make_state):
    valid, invalid = checked(spark, [make_state()])
    assert valid.count() == 1 and invalid.count() == 0
    assert "reject_reasons" not in valid.columns


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"icao24": None}, "icao24_invalid"),
        ({"icao24": "xyz123"}, "icao24_invalid"),
        ({"last_contact": None}, "last_contact_missing"),
        ({"latitude": 95.0}, "latitude_invalid"),
        ({"latitude": None}, "latitude_invalid"),
        ({"longitude": -200.0}, "longitude_invalid"),
        ({"longitude": None}, "longitude_invalid"),
        ({"velocity": -5.0}, "velocity_negative"),
        ({"baro_altitude": 99999.0}, "altitude_out_of_range"),
    ],
)
def test_each_rule_rejects_with_its_reason(spark, make_state, overrides, reason):
    valid, invalid = checked(spark, [make_state(**overrides)])
    assert valid.count() == 0
    assert reason in invalid.first()["reject_reasons"]


def test_optional_fields_may_be_null(spark, make_state):
    valid, _ = checked(spark, [make_state(velocity=None, baro_altitude=None)])
    assert valid.count() == 1


def test_all_broken_rules_are_listed(spark, make_state):
    _, invalid = checked(spark, [make_state(latitude=95.0, velocity=-1.0)])
    assert set(invalid.first()["reject_reasons"]) == {"latitude_invalid", "velocity_negative"}


def test_good_and_bad_rows_are_split(spark, make_state):
    valid, invalid = checked(spark, [make_state(), make_state(icao24="aaaaaa", latitude=99.0)])
    assert valid.count() == 1 and invalid.count() == 1
