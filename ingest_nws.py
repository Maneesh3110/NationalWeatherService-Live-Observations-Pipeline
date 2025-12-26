import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

BASE_DIR = Path(__file__).parent.resolve()
INPUT_DIR = BASE_DIR / "data" / "input" / "json_stream"
INPUT_DIR.mkdir(parents=True, exist_ok=True)

STATIONS = [
    s.strip()
    for s in os.getenv("NWS_STATIONS", "KCVG").split(",")
    if s.strip()
]
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "60"))
USER_AGENT = os.getenv("NWS_USER_AGENT", "").strip()

if not USER_AGENT:
    raise ValueError(
        "NWS_USER_AGENT is required (e.g., 'maneeshreddyyanala@gmail.com nws-observer/1.0')."
    )

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "application/geo+json"})


def fetch_station_observation(station_id: str) -> Optional[Dict[str, Any]]:
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    resp = SESSION.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def normalize_record(station_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        props = payload.get("properties", {})
        temp_c = props.get("temperature", {}).get("value")
        rh = props.get("relativeHumidity", {}).get("value")
        ts = props.get("timestamp")
        coords = (
            payload.get("geometry", {}) or {}
        ).get("coordinates", [])
        lon = coords[0] if len(coords) >= 1 else None
        lat = coords[1] if len(coords) >= 2 else None
        if temp_c is None or rh is None or ts is None:
            return None
        return {
            "station_id": station_id,         # preserve originating station
            "temperature": float(temp_c),     # Celsius
            "humidity": float(rh),            # %
            "timestamp": ts,                  # ISO8601 string (UTC)
            "latitude": float(lat) if lat is not None else None,
            "longitude": float(lon) if lon is not None else None,
        }
    except Exception:
        return None


def write_ndjson(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    file_path = INPUT_DIR / f"batch_{ts}.json"
    with file_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(records)} records -> {file_path}")


def main():
    print(
        f"Polling NWS stations {STATIONS} every {POLL_SECONDS}s ... "
        "(Ctrl+C to stop)"
    )
    while True:
        batch: List[Dict[str, Any]] = []
        for st in STATIONS:
            try:
                payload = fetch_station_observation(st)
                rec = normalize_record(st, payload) if payload else None
                if rec:
                    batch.append(rec)
                else:
                    print(f"[warn] Missing/invalid fields for station {st}")
            except Exception as e:
                print(f"[warn] station {st} fetch failed: {e}")
        if batch:
            write_ndjson(batch)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()