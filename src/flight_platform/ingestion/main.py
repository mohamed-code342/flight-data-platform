"""Fetch ONE snapshot from OpenSky. Scheduling is Airflow's job, not this script's.

Run:  python -m flight_platform.ingestion.main [--limit 5] [--out snapshot.json]
"""

import argparse
import json
from pathlib import Path

from flight_platform.ingestion.opensky_client import OpenSkyClient, OpenSkyError
from flight_platform.log_config import get_logger

logger = get_logger(__name__)

PREVIEW_FIELDS = ("icao24", "callsign", "origin_country", "latitude", "longitude", "baro_altitude")


def fetch_snapshot() -> dict:
    return OpenSkyClient().get_states()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch one flight-states snapshot from OpenSky")
    parser.add_argument("--limit", type=int, default=5, help="how many rows to preview")
    parser.add_argument("--out", type=Path, help="save the full snapshot to this JSON file")
    args = parser.parse_args(argv)

    try:
        snapshot = fetch_snapshot()
    except OpenSkyError as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1

    states = snapshot["states"]
    logger.info("Snapshot time=%s, aircraft=%d", snapshot["time"], len(states))
    for state in states[: args.limit]:
        print({k: state.get(k) for k in PREVIEW_FIELDS})

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(snapshot), encoding="utf-8")
        logger.info("Saved snapshot to %s", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
