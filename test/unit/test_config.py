from flight_platform.config import PROJECT_ROOT, settings
from flight_platform.log_config import get_logger


def test_default_paths_are_inside_project():
    assert settings.bronze_dir == PROJECT_ROOT / "data" / "bronze"
    assert settings.silver_dir.name == "silver"
    assert settings.gold_dir.name == "gold"
    assert settings.quarantine_dir.name == "quarantine"


def test_defaults_are_sane():
    assert settings.opensky_poll_interval_seconds > 0
    assert settings.sqlserver_port == 1433


def test_logger_works():
    get_logger("flight_platform.test").info("logger ok")
