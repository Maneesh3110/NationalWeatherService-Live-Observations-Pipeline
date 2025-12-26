import glob
import time
from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

BASE_DIR = Path(__file__).parent.resolve()
CRITICAL_PATH = BASE_DIR / "data" / "output" / "critical"
AVG_PATH = BASE_DIR / "data" / "output" / "avg"
HUMIDITY_PATH = BASE_DIR / "data" / "output" / "humidity"
BASELINE_PATH = BASE_DIR / "data" / "output" / "baselines"

REFRESH_SECS = 5
LOCAL_TZ = datetime.now().astimezone().tzinfo

st.set_page_config(page_title="NWS Live Observations", layout="wide")
st.title("🌤️ National Weather Service Live Observations Pipeline")

st.markdown(
    """
    <style>
    body {
        background-color: #030711;
        color: #f5f6fb;
    }
    .metric-card {
        background: linear-gradient(135deg, rgba(43,104,255,0.25), rgba(223,72,251,0.12));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 16px 20px;
        box-shadow: 0 15px 40px rgba(5,8,20,0.45);
        backdrop-filter: blur(8px);
        min-height: 120px;
    }
    .metric-label {
        font-size: 0.9rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #c8d1f5;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 600;
        margin-top: 6px;
    }
    .metric-trend {
        margin-top: 4px;
        font-size: 0.95rem;
        color: #9fb4ff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def latest_parquet_df(path: Path) -> pd.DataFrame:
    files = sorted(glob.glob(str(path / "*.parquet")))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])


def to_local_time(df: pd.DataFrame, columns):
    if df.empty:
        return df
    localized = df.copy()
    for column in columns:
        if column in localized.columns:
            localized[column] = (
                pd.to_datetime(localized[column], utc=True, errors="coerce")
                .dt.tz_convert(LOCAL_TZ)
            )
    return localized


def freshest_baseline(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "station_id" not in df.columns:
        return pd.DataFrame()
    ordered = df.sort_values("window_start", ascending=False)
    return ordered.drop_duplicates(subset=["station_id"])


def render_metric_cards(avg_df: pd.DataFrame, critical_df: pd.DataFrame):
    columns = st.columns(4)

    def render_card(col, label, value, trend=""):
        col.markdown(
            f"""
            <div class='metric-card'>
                <div class='metric-label'>{label}</div>
                <div class='metric-value'>{value}</div>
                <div class='metric-trend'>{trend}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if not avg_df.empty and {"station_id", "avg_temperature"}.issubset(avg_df.columns):
        hottest = avg_df.loc[avg_df["avg_temperature"].idxmax()]
        coldest = avg_df.loc[avg_df["avg_temperature"].idxmin()]
        humid = avg_df.loc[avg_df["avg_humidity"].idxmax()]
        render_card(columns[0], "Hottest station", f"{hottest['avg_temperature']:.1f}°C", hottest["station_id"])
        render_card(columns[1], "Coldest station", f"{coldest['avg_temperature']:.1f}°C", coldest["station_id"])
        render_card(columns[2], "Highest humidity", f"{humid['avg_humidity']:.1f}%", humid["station_id"])
    else:
        for col in columns[:3]:
            render_card(col, "Awaiting data", "—")

    active_alerts = len(critical_df) if not critical_df.empty else 0
    render_card(columns[3], "Active alerts", str(active_alerts), "stations crossing thresholds")


def render_station_map(avg_df: pd.DataFrame):
    if avg_df.empty or not {"latitude", "longitude"}.issubset(avg_df.columns):
        st.info("Map populates once averages land.")
        return
    map_df = avg_df.dropna(subset=["latitude", "longitude"]).copy()
    if map_df.empty:
        st.info("No geocoded stations yet.")
        return
    map_df["temp_color"] = map_df["avg_temperature"].clip(-20, 45)
    map_df["color_r"] = ((map_df["temp_color"] + 20) / 65 * 255).clip(0, 255).astype(int)
    map_df["color_b"] = (255 - map_df["color_r"]).astype(int)
    view_state = pdk.ViewState(
        latitude=float(map_df["latitude"].mean()),
        longitude=float(map_df["longitude"].mean()),
        zoom=4,
        pitch=40,
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[longitude, latitude]",
        get_radius="50000",
        get_fill_color="[color_r, 90, color_b, 220]",
        pickable=True,
    )
    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            layers=[layer],
            initial_view_state=view_state,
            tooltip={
                "html": "<b>{station_id}</b><br/>Temp: {avg_temperature} °C<br/>Humidity: {avg_humidity}%",
                "style": {"color": "#fff"},
            },
        )
    )


def render_alert_timeline(critical_df: pd.DataFrame):
    if critical_df.empty or "timestamp" not in critical_df.columns:
        st.info("Alert timeline populates once extremes arrive.")
        return
    timeline_df = critical_df.copy()
    timeline_df["timestamp_local"] = pd.to_datetime(timeline_df["timestamp"], errors="coerce")
    if timeline_df["timestamp_local"].dt.tz is None:
        timeline_df["timestamp_local"] = timeline_df["timestamp_local"].dt.tz_localize(LOCAL_TZ)
    else:
        timeline_df["timestamp_local"] = timeline_df["timestamp_local"].dt.tz_convert(LOCAL_TZ)
    timeline_df["timestamp_local"] = timeline_df["timestamp_local"].dt.tz_localize(None)
    timeline_df = timeline_df.dropna(subset=["timestamp_local"])
    if timeline_df.empty:
        st.info("Alert timeline populates once extremes arrive.")
        return
    chart = (
        alt.Chart(timeline_df)
        .mark_circle(size=140)
        .encode(
            x=alt.X("timestamp_local:T", title="Local time"),
            y=alt.Y("temperature:Q", title="Temperature (°C)"),
            color=alt.Color("severity:N", legend=alt.Legend(title="Severity")),
            tooltip=["station_id", "temperature", "humidity", "severity", "alert_reason", "timestamp_local"],
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


st.caption(f"Auto-refresh every {REFRESH_SECS}s")

baseline_df = to_local_time(latest_parquet_df(BASELINE_PATH), ["window_start", "window_end"])
baseline_latest = freshest_baseline(baseline_df)
critical_df = to_local_time(latest_parquet_df(CRITICAL_PATH), ["timestamp"])
avg_df = to_local_time(latest_parquet_df(AVG_PATH), ["window_start", "window_end"])
hum_df = latest_parquet_df(HUMIDITY_PATH)

if not critical_df.empty and not baseline_latest.empty:
    critical_df = critical_df.merge(
        baseline_latest[["station_id", "avg_temperature_7d", "avg_humidity_7d"]],
        on="station_id",
        how="left",
    )
    critical_df["temp_vs_7d"] = critical_df["temperature"] - critical_df["avg_temperature_7d"]
    critical_df["humidity_vs_7d"] = critical_df["humidity"] - critical_df["avg_humidity_7d"]

render_metric_cards(avg_df, critical_df)

st.divider()

critical_col, map_col = st.columns((1.3, 0.7))
with critical_col:
    st.subheader("Critical Temperatures (Heat ≥32°C / Wind Chill ≤-12°C)")
    if critical_df.empty:
        st.info("Waiting for streaming data...")
    else:
        st.dataframe(critical_df.sort_values("timestamp", ascending=False), use_container_width=True)

with map_col:
    st.subheader("Geo Snapshot")
    render_station_map(avg_df)

st.divider()

avg_col, timeline_col = st.columns((1.1, 0.9))
with avg_col:
    st.subheader("1-minute Rolling Averages (per station)")
    if avg_df.empty:
        st.info("Waiting for streaming data...")
    else:
        st.dataframe(avg_df.sort_values("window_start", ascending=False), use_container_width=True)
        if {"station_id", "avg_temperature"}.issubset(avg_df.columns):
            st.bar_chart(avg_df.set_index("station_id")["avg_temperature"])

with timeline_col:
    st.subheader("Alert Timeline")
    render_alert_timeline(critical_df)

st.divider()

st.subheader("Stations Needing Attention by Humidity (<45% or >75%)")
if hum_df.empty:
    st.info("Waiting for streaming data...")
else:
    st.dataframe(hum_df.sort_values("critical_readings", ascending=True), use_container_width=True)
    if {"station_id", "critical_readings"}.issubset(hum_df.columns):
        st.bar_chart(hum_df.set_index("station_id")["critical_readings"])

st.divider()

st.subheader("7-day Station Baselines")
if baseline_latest.empty:
    st.info("Baselines populate after at least one hour of data.")
else:
    st.dataframe(
        baseline_latest.sort_values("window_start", ascending=False),
        use_container_width=True,
    )

time.sleep(REFRESH_SECS)
st.rerun()