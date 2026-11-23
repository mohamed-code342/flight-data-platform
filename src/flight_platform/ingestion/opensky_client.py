"""OpenSky Network client.

Responsibilities: talk to the API (OAuth2 when credentials exist), retry on
transient failures, and respect rate limits. It knows nothing about storage.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests

from flight_platform.config import Settings
from flight_platform.config import settings as default_settings
from flight_platform.log_config import get_logger

logger = get_logger(__name__)

# Order of the columns in each state-vector row returned by /states/all
STATE_FIELDS = (
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",  # only present when extended=1
)

MAX_RATE_LIMIT_WAIT = 120  # seconds: longer than this we fail instead of sleeping
MAX_BACKOFF = 30  # seconds
TOKEN_EXPIRY_MARGIN = 60  # refresh the token this many seconds before it expires


class OpenSkyError(Exception):
    """Raised when OpenSky cannot be reached or returns an unusable answer."""


@dataclass(frozen=True)
class BoundingBox:
    lamin: float
    lomin: float
    lamax: float
    lomax: float

    @classmethod
    def parse(cls, text: str) -> BoundingBox:
        try:
            lamin, lomin, lamax, lomax = (float(p) for p in text.split(","))
        except ValueError as exc:
            raise ValueError(f"Expected 'lamin,lomin,lamax,lomax', got {text!r}") from exc
        if lamin >= lamax or lomin >= lomax:
            raise ValueError(f"Each min value must be smaller than its max: {text!r}")
        return cls(lamin, lomin, lamax, lomax)

    def as_params(self) -> dict[str, float]:
        return {
            "lamin": self.lamin,
            "lomin": self.lomin,
            "lamax": self.lamax,
            "lomax": self.lomax,
        }


class OpenSkyClient:
    def __init__(
        self,
        cfg: Settings | None = None,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg or default_settings
        self.session = session or requests.Session()
        self._sleep = sleep
        self._clock = clock
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._last_request_at: float | None = None

    @property
    def authenticated(self) -> bool:
        return bool(self.cfg.opensky_client_id and self.cfg.opensky_client_secret)

    # ------------------------------------------------------------------ public
    def get_states(self, bbox: BoundingBox | None = None, extended: bool = True) -> dict[str, Any]:
        """Fetch one snapshot. Returns {"time": int | None, "states": [dict, ...]}."""
        if bbox is None and self.cfg.opensky_bbox:
            bbox = BoundingBox.parse(self.cfg.opensky_bbox)

        params: dict[str, Any] = {"extended": 1} if extended else {}
        if bbox:
            params.update(bbox.as_params())

        url = f"{self.cfg.opensky_base_url}/states/all"
        token_refreshed = False
        last_error = "unknown error"

        for attempt in range(self.cfg.opensky_max_retries + 1):
            self._throttle()
            headers = {}
            token = self._get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"

            try:
                resp = self.session.get(
                    url, params=params, headers=headers, timeout=self.cfg.opensky_timeout_seconds
                )
            except requests.RequestException as exc:
                last_error = f"network error: {exc}"
                self._backoff(attempt, last_error)
                continue

            if resp.status_code == 200:
                return self._parse(resp)

            if resp.status_code == 401 and self.authenticated and not token_refreshed:
                logger.info("Token rejected, requesting a new one")
                token_refreshed = True
                self._get_token(force=True)
                continue

            if resp.status_code == 429:
                wait = self._retry_after(resp)
                if wait is not None and wait > MAX_RATE_LIMIT_WAIT:
                    raise OpenSkyError(
                        f"Rate limit reached (API credits exhausted); retry after {wait:.0f}s"
                    )
                last_error = "rate limited (HTTP 429)"
                self._backoff(attempt, last_error, delay=wait)
                continue

            if resp.status_code >= 500:
                last_error = f"server error (HTTP {resp.status_code})"
                self._backoff(attempt, last_error)
                continue

            # Other 4xx errors will not fix themselves, so do not retry them.
            raise OpenSkyError(f"HTTP {resp.status_code}: {resp.text[:200]}")

        raise OpenSkyError(
            f"Giving up after {self.cfg.opensky_max_retries + 1} attempts: {last_error}"
        )

    # ----------------------------------------------------------------- helpers
    def _parse(self, resp: requests.Response) -> dict[str, Any]:
        try:
            data = resp.json()
        except ValueError as exc:
            raise OpenSkyError("OpenSky returned invalid JSON") from exc
        rows = data.get("states") or []  # "states" is null when no aircraft are tracked
        states = [dict(zip(STATE_FIELDS, row, strict=False)) for row in rows]
        logger.info("Fetched %d aircraft states", len(states))
        return {"time": data.get("time"), "states": states}

    def _get_token(self, force: bool = False) -> str | None:
        if not self.authenticated:
            return None
        if not force and self._token and self._clock() < self._token_expires_at:
            return self._token
        try:
            resp = self.session.post(
                self.cfg.opensky_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.cfg.opensky_client_id,
                    "client_secret": self.cfg.opensky_client_secret,
                },
                timeout=self.cfg.opensky_timeout_seconds,
            )
        except requests.RequestException as exc:
            raise OpenSkyError(f"Could not reach the OpenSky auth server: {exc}") from exc
        if resp.status_code != 200:
            raise OpenSkyError(f"Token request failed (HTTP {resp.status_code})")
        payload = resp.json()
        self._token = payload["access_token"]
        lifetime = payload.get("expires_in", 1800)
        self._token_expires_at = self._clock() + max(lifetime - TOKEN_EXPIRY_MARGIN, 0)
        return self._token

    def _throttle(self) -> None:
        """Keep a minimum pause between two API calls."""
        if self._last_request_at is not None:
            wait = self.cfg.opensky_min_interval_seconds - (self._clock() - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._clock()

    def _backoff(self, attempt: int, reason: str, delay: float | None = None) -> None:
        if attempt >= self.cfg.opensky_max_retries:
            return  # that was the last attempt, no point in waiting
        delay = delay if delay is not None else min(2 ** (attempt + 1), MAX_BACKOFF)
        logger.warning("%s; retrying in %.0fs (attempt %d)", reason, delay, attempt + 1)
        self._sleep(delay)

    @staticmethod
    def _retry_after(resp: requests.Response) -> float | None:
        value = resp.headers.get("X-Rate-Limit-Retry-After-Seconds")
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None
