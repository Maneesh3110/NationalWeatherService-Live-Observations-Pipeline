# National Weather Service Live Observations Pipeline

Near-real-time weather intelligence pipeline powered by the **National Weather Service (NWS)**. A lightweight Python ingestor pulls observations, **Spark Structured Streaming** derives actionable signals, and a **Streamlit** dashboard visualizes temperature and humidity trends for the stations you monitor.

## Architecture

```
NWS API (latest observation per station)
        ↓  ingest_nws.py  (polls, validates)
data/input/json_stream/*.json  (NDJSON landing zone)
        ↓  spark_app.py  (Structured Streaming)
data/output/{critical,avg,humidity}  (Parquet snapshots)
        ↓  streamlit_app.py  (auto-refresh dashboard)
```

Spark operates solely on landed files—no direct API calls. This makes development and replay straightforward and allows the dashboard to rehydrate from historical parquet data.

## Features

- Configurable NWS station list, poll interval, and user agent (required by NWS policy).
- Optional synthetic rate source for offline demos via `USE_MOCK_SOURCE=1`.
- Station metadata (latitude/longitude) captured from the GeoJSON payload so you can render geospatial views instantly.
- Structured Streaming outputs:
  - **Critical Weather Events**: flags readings that cross NWS advisory thresholds (heat ≥32 °C / wind chill ≤−12 °C) and calculates heat index.
  - **Rolling Averages**: 1‑minute aggregates per station for quick trend inspection.
  - **Humidity Attention**: highlights stations with humidity outside 45‑75 %.
  - **7‑Day Baselines**: continuously refreshed climatology snapshots per station for anomaly comparisons.
- Streamlit dashboard includes hero metrics, live alert timeline, and an interactive station map (via PyDeck) for a futuristic presentation layer.
- Streamlit UI auto-refreshes every few seconds and charts both temperature and humidity trends.

## Prerequisites

- **Python**: 3.11 or 3.12 (project developed/tested on 3.12.7).
- **Java (HotSpot)**: JDK 11 (LTS) such as Azul Zulu 11 (`brew install --cask zulu11`). Spark 3.5.x requires a HotSpot build; IBM Semeru/OpenJ9 is not compatible because it lacks the JAAS modules Spark expects.
- **pip** and **virtualenv** available on your system.

Verify your toolchain:

```bash
python --version
java -version      # should show Zulu/HotSpot, not OpenJ9
```

## Setup

1. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```
2. **Upgrade packaging tools & install dependencies**
   ```bash
   python -m pip install --upgrade pip setuptools wheel
   pip install -r requirements.txt
   ```
3. **Configure Java**
   ```bash
   export JAVA_HOME=$(/usr/libexec/java_home -v 11)
   export PATH="$JAVA_HOME/bin:$PATH"
   ```
4. **Export required environment variables** (examples shown):
   ```bash
   export NWS_STATIONS="KCVG,KSFO"
   export POLL_SECONDS=60
   export NWS_USER_AGENT="you@example.com nws-pipeline/1.0"  # REQUIRED by NWS
   # Optional overrides
   export USE_MOCK_SOURCE=0
   export PYSPARK_FORCE_SIMPLE_AUTH=1
   ```

## Running the Pipeline

1. **Start the NWS ingestor** (writes NDJSON files under `data/input/json_stream/`):
   ```bash
   python ingest_nws.py
   ```
2. **Launch the Spark streaming job** (new terminal, same venv/env vars):
   ```bash
   python spark_app.py
   # or run with synthetic data
   # USE_MOCK_SOURCE=1 python spark_app.py
   ```
   Outputs are continuously written to `data/output/critical`, `data/output/avg`, and `data/output/humidity` with checkpoints under `data/checkpoints/`.
   The baselines stream adds `data/output/baselines`.
3. **Serve the Streamlit dashboard** (another terminal):
   ```bash
   streamlit run streamlit_app.py
   ```
   Visit http://localhost:8501 to monitor the live metrics. Stop any component with `Ctrl+C`.

## Configuration Reference

| Variable                                             | Purpose                                                                         | Default             |
| ---------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------- |
| `NWS_STATIONS`                                       | Comma-separated list of station IDs to poll.                                    | `KCVG`              |
| `POLL_SECONDS`                                       | Poll interval for the ingestor (seconds). Respect NWS rate guidance.            | `60`                |
| `NWS_USER_AGENT`                                     | Required contact string per NWS policy (`name@email project/version`).          | **none → required** |
| `USE_MOCK_SOURCE`                                    | `1` to bypass landed files and use Spark’s rate source.                         | `0`                 |
| `PYSPARK_FORCE_SIMPLE_AUTH`                          | Keeps Hadoop auth in `simple` mode to avoid Kerberos lookups on local machines. | `1`                 |
| `PYSPARK_FORCE_ADD_OPENS` / `PYSPARK_SKIP_ADD_OPENS` | Manually control Spark’s `--add-opens` flags when using Java 17+.               | unset               |

## Data Layout

```
data/
├─ input/
│  └─ json_stream/     # NDJSON drops from ingest_nws.py
├─ output/
│  ├─ critical/        # Per-reading anomalies (append mode)
│  ├─ avg/             # Rolling averages (overwrite per batch)
│  └─ humidity/        # Humidity attention summary
├─ baselines/          # 7-day running averages per station
└─ checkpoints/        # Structured Streaming checkpoints per sink
```

## Dashboard Screenshots

| Landing | 1-minute Rolling Averages (per station) |
| --- | --- |
| ![Landing screen](https://github.com/Maneesh3110/NationalWeatherService-Live-Observations-Pipeline/blob/main/output/screenshots/Landing_screen_1.png) | ![1-minute Rolling Averages (per station)](https://github.com/Maneesh3110/NationalWeatherService-Live-Observations-Pipeline/blob/main/output/screenshots/1-minute%20Rolling%20Averages%20(per%20station).png) |

| Stations Needing Attention by Humidity | 7-day Station Baselines |
| --- | --- |
| ![Stations Needing Attention by Humidity](https://github.com/Maneesh3110/NationalWeatherService-Live-Observations-Pipeline/blob/main/output/screenshots/Stations%20Needing%20Attention%20by%20Humidity.png) | ![7-day Station Baselines](https://github.com/Maneesh3110/NationalWeatherService-Live-Observations-Pipeline/blob/main/output/screenshots/7-day%20Station%20Baselines.png) |



## Troubleshooting

- **JAAS / LoginModule errors**: Ensure you are running a HotSpot JDK (Temurin, Zulu, etc.). IBM Semeru/OpenJ9 lacks the `com.ibm.security.auth` modules Spark expects.
- **Watchdog warning in Streamlit**: Install Apple command-line tools (`xcode-select --install`) and `pip install watchdog` for faster file watching. The app works without it; refresh may be slower.
- **No data in dashboard**: Confirm the ingestor is running and writing files, or set `USE_MOCK_SOURCE=1` to drive synthetic data.
- **Schema errors**: Delete `data/checkpoints/*` when you change schemas; Spark will rebuild state.

## Next Steps

- Swap file landing for Kafka/Kinesis if you need higher throughput or exactly-once semantics.
- Package the stack via Docker/Compose for reproducible deployments.
- Add CI (lint/type-check) and automated tests around the ingestion and transformation logic.
