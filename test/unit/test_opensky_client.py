import pytest
import requests

from flight_platform.config import Settings
from flight_platform.ingestion.opensky_client import (
    STATE_FIELDS,
    BoundingBox,
    OpenSkyClient,
    OpenSkyError,
)


class FakeResponse:
    def __init__(self, status=200, payload=None, headers=None, text=""):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    """Plays back queued responses for GET; POST (token) has its own queue."""

    def __init__(self, gets=(), posts=()):
        self.gets = list(gets)
        self.posts = list(posts)
        self.get_calls = []
        self.post_calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.get_calls.append({"url": url, "params": params, "headers": headers})
        item = self.gets.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def post(self, url, data=None, timeout=None):
        self.post_calls.append({"url": url, "data": data})
        return self.posts.pop(0)


class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(session, clock=None, **overrides):
    clock = clock or FakeClock()
    values = {
        "opensky_client_id": None,
        "opensky_client_secret": None,
        "opensky_bbox": None,
        "opensky_min_interval_seconds": 0,
        "opensky_max_retries": 3,
    }
    values.update(overrides)
    cfg = Settings(_env_file=None, **values)
    client = OpenSkyClient(cfg, session=session, sleep=clock.sleep, clock=clock.time)
    return client, clock


ROW = ["3c6444", "DLH123  ", "Germany", 1700000000, 1700000001, 8.5, 50.0, 10000.0,
       False, 230.0, 90.0, 0.0, None, 10100.0, "1000", False, 0, 3]  # fmt: skip
OK = FakeResponse(200, {"time": 1700000001, "states": [ROW]})


def test_rows_become_named_dicts():
    client, _ = make_client(FakeSession(gets=[OK]))
    snapshot = client.get_states()
    assert snapshot["time"] == 1700000001
    state = snapshot["states"][0]
    assert tuple(state) == STATE_FIELDS
    assert state["icao24"] == "3c6444"
    assert state["latitude"] == 50.0


def test_null_states_means_empty_list():
    client, _ = make_client(FakeSession(gets=[FakeResponse(200, {"time": 1, "states": None})]))
    assert client.get_states()["states"] == []


def test_retries_server_errors_with_backoff():
    session = FakeSession(gets=[FakeResponse(503), FakeResponse(500), OK])
    client, clock = make_client(session)
    assert len(client.get_states()["states"]) == 1
    assert len(session.get_calls) == 3
    assert clock.sleeps == [2, 4]  # exponential backoff


def test_retries_network_errors_then_gives_up():
    session = FakeSession(gets=[requests.ConnectionError("boom")] * 4)
    client, _ = make_client(session)
    with pytest.raises(OpenSkyError, match="Giving up after 4 attempts"):
        client.get_states()
    assert len(session.get_calls) == 4


def test_429_waits_for_retry_after_header():
    limited = FakeResponse(429, headers={"X-Rate-Limit-Retry-After-Seconds": "7"})
    client, clock = make_client(FakeSession(gets=[limited, OK]))
    client.get_states()
    assert clock.sleeps == [7]


def test_429_with_long_wait_fails_fast():
    exhausted = FakeResponse(429, headers={"X-Rate-Limit-Retry-After-Seconds": "40000"})
    session = FakeSession(gets=[exhausted])
    client, clock = make_client(session)
    with pytest.raises(OpenSkyError, match="credits exhausted"):
        client.get_states()
    assert clock.sleeps == [] and len(session.get_calls) == 1


def test_client_errors_are_not_retried():
    session = FakeSession(gets=[FakeResponse(404, text="not found")])
    client, _ = make_client(session)
    with pytest.raises(OpenSkyError, match="HTTP 404"):
        client.get_states()
    assert len(session.get_calls) == 1


def test_oauth_token_is_sent_and_reused():
    token = FakeResponse(200, {"access_token": "abc", "expires_in": 1800})
    session = FakeSession(gets=[OK, OK], posts=[token])
    client, _ = make_client(session, opensky_client_id="id", opensky_client_secret="secret")
    client.get_states()
    client.get_states()
    assert session.get_calls[0]["headers"]["Authorization"] == "Bearer abc"
    assert len(session.post_calls) == 1  # token reused, not re-requested
    assert session.post_calls[0]["data"]["grant_type"] == "client_credentials"


def test_expired_token_is_refreshed_on_401():
    t1 = FakeResponse(200, {"access_token": "old", "expires_in": 1800})
    t2 = FakeResponse(200, {"access_token": "new", "expires_in": 1800})
    session = FakeSession(gets=[FakeResponse(401), OK], posts=[t1, t2])
    client, _ = make_client(session, opensky_client_id="id", opensky_client_secret="secret")
    client.get_states()
    assert session.get_calls[1]["headers"]["Authorization"] == "Bearer new"


def test_anonymous_requests_send_no_auth_header():
    session = FakeSession(gets=[OK])
    client, _ = make_client(session)
    client.get_states()
    assert "Authorization" not in session.get_calls[0]["headers"]


def test_min_interval_between_calls():
    session = FakeSession(gets=[OK, OK])
    client, clock = make_client(session, opensky_min_interval_seconds=10)
    client.get_states()
    client.get_states()
    assert clock.sleeps == [10]


def test_bbox_from_settings_goes_into_query():
    session = FakeSession(gets=[OK])
    client, _ = make_client(session, opensky_bbox="22,24,32,37")
    client.get_states()
    params = session.get_calls[0]["params"]
    assert params["lamin"] == 22 and params["lomax"] == 37 and params["extended"] == 1


def test_bbox_validation():
    assert BoundingBox.parse("1,2,3,4").lomax == 4
    with pytest.raises(ValueError):
        BoundingBox.parse("1,2,3")
    with pytest.raises(ValueError):
        BoundingBox.parse("10,2,3,4")
