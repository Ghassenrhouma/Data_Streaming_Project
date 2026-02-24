"""
✈️  OpenSky Real-Time Flight Monitor — Streamlit Dashboard
==========================================================
Reads the latest JSON files produced by the Spark Structured
Streaming pipeline.  Refresh manually or enable auto-refresh
from the sidebar.

Run with:
    streamlit run dashboard/app.py
"""

import glob
import json
import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

# ── Page configuration ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="✈️ OpenSky Real-Time Flight Monitor",
    page_icon="✈️",
    layout="wide",
)

# ── Output paths (relative to project root) ─────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR  = os.path.join(BASE_DIR, "output", "cleaned_flights")
ANOMALIES_DIR = os.path.join(BASE_DIR, "output", "anomalies")
WINDOWED_DIR  = os.path.join(BASE_DIR, "output", "windowed_stats")

REFRESH_INTERVAL = 15  # seconds (used for auto-refresh & cache TTL)


# ── Sidebar controls ────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Refresh Settings")
    auto_refresh = st.toggle("Auto-refresh", value=False,
                             help="Automatically reload data every 15 s")
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown(
        f"**Refresh interval:** {REFRESH_INTERVAL} s\n\n"
        f"**Data directory:** `output/`"
    )


# ── Helper: newest file timestamp in a directory ────────────────────────────
def _newest_mtime(directory: str) -> str | None:
    """Return the modification time of the newest file in *directory*."""
    try:
        files = glob.glob(os.path.join(directory, "*"))
        if not files:
            return None
        newest = max(files, key=os.path.getmtime)
        return datetime.fromtimestamp(os.path.getmtime(newest)).strftime("%H:%M:%S")
    except OSError:
        return None


# ── Data loaders ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_cleaned_flights() -> pd.DataFrame:
    """Read the most recent cleaned-flight JSON files."""
    files = sorted(glob.glob(os.path.join(CLEANED_DIR, "part-*.json")))
    if not files:
        return pd.DataFrame()
    # Read only the last 5 part files to keep it fast
    recent = files[-5:]
    dfs = []
    for f in recent:
        try:
            if os.path.getsize(f) > 0:
                dfs.append(pd.read_json(f, lines=True))
        except (ValueError, pd.errors.ParserError):
            pass  # skip corrupt / partially-written files
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_anomalies() -> pd.DataFrame:
    """Read anomaly detection results (newline-delimited JSON)."""
    files = sorted(glob.glob(os.path.join(ANOMALIES_DIR, "batch_*.json")))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            if os.path.getsize(f) > 0:
                dfs.append(pd.read_json(f, lines=True))
        except (ValueError, pd.errors.ParserError):
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_windowed_stats() -> pd.DataFrame:
    """Read windowed aggregate stats."""
    files = sorted(glob.glob(os.path.join(WINDOWED_DIR, "part-*.json")))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            if os.path.getsize(f) > 0:
                dfs.append(pd.read_json(f, lines=True))
        except (ValueError, pd.errors.ParserError):
            pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    # Parse window timestamps
    for col in ("window_start", "window_end"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df.sort_values("window_start")


# ── Load data ────────────────────────────────────────────────────────────────
flights = load_cleaned_flights()
anomalies = load_anomalies()
windowed = load_windowed_stats()

# ── Title + freshness banner ────────────────────────────────────────────────
st.title("✈️ OpenSky Real-Time Flight Monitor")

# Show data-freshness indicators
fresh_flights = _newest_mtime(CLEANED_DIR)
fresh_anomaly = _newest_mtime(ANOMALIES_DIR)
fresh_window  = _newest_mtime(WINDOWED_DIR)
now_str = pd.Timestamp.now().strftime("%H:%M:%S")
st.caption(
    f"🕒 Dashboard rendered: **{now_str}**  ·  "
    f"Flights file: **{fresh_flights or '—'}**  ·  "
    f"Anomalies file: **{fresh_anomaly or '—'}**  ·  "
    f"Windowed file: **{fresh_window or '—'}**"
)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — KPI Metrics Row  (with deltas from previous run)
# ═══════════════════════════════════════════════════════════════════════════════

# Compute current values
cur_flights   = len(flights) if not flights.empty else 0
cur_anomalies = int(anomalies["is_anomaly"].sum()) if (not anomalies.empty and "is_anomaly" in anomalies.columns) else 0
cur_alt       = flights["baro_altitude"].mean() if not flights.empty else 0.0
cur_vel       = flights["velocity"].mean() if not flights.empty else 0.0

# Retrieve previous values from session_state (for deltas)
prev = st.session_state.get("prev_kpis", {})
delta_flights   = cur_flights   - prev.get("flights", cur_flights)
delta_anomalies = cur_anomalies - prev.get("anomalies", cur_anomalies)
delta_alt       = cur_alt       - prev.get("alt", cur_alt)
delta_vel       = cur_vel       - prev.get("vel", cur_vel)

# Save current as previous for next refresh
st.session_state["prev_kpis"] = {
    "flights": cur_flights, "anomalies": cur_anomalies,
    "alt": cur_alt, "vel": cur_vel,
}

st.markdown("---")
k1, k2, k3, k4 = st.columns(4)

with k1:
    st.metric("🛫 Active Flights", f"{cur_flights:,}",
              delta=f"{delta_flights:+,}" if delta_flights else None)
with k2:
    st.metric("🚨 Anomalies Detected", f"{cur_anomalies:,}",
              delta=f"{delta_anomalies:+,}" if delta_anomalies else None,
              delta_color="inverse")
with k3:
    st.metric("📏 Avg Altitude (m)", f"{cur_alt:,.0f}",
              delta=f"{delta_alt:+,.0f}" if delta_alt else None)
with k4:
    st.metric("💨 Avg Velocity (m/s)", f"{cur_vel:,.1f}",
              delta=f"{delta_vel:+,.1f}" if delta_vel else None)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Live Flight Map
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🗺️ Live Flight Map")

if not flights.empty and "latitude" in flights.columns and "longitude" in flights.columns:
    map_df = flights[["latitude", "longitude", "callsign", "origin_country",
                       "baro_altitude", "velocity"]].copy()
    map_df = map_df.dropna(subset=["latitude", "longitude"])

    # Mark anomalies on the map
    anomaly_icao = set()
    if not anomalies.empty and "is_anomaly" in anomalies.columns:
        anomaly_callsigns = set(anomalies.loc[
            anomalies["is_anomaly"] | anomalies.get("rule_based_anomaly", False),
            "callsign"
        ].dropna())
    else:
        anomaly_callsigns = set()

    map_df["is_anomaly"] = map_df["callsign"].isin(anomaly_callsigns)
    map_df["color_r"] = map_df["is_anomaly"].apply(lambda x: 220 if x else 30)
    map_df["color_g"] = map_df["is_anomaly"].apply(lambda x: 50 if x else 130)
    map_df["color_b"] = map_df["is_anomaly"].apply(lambda x: 50 if x else 230)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=json.loads(map_df.to_json(orient="records")),
        get_position=["longitude", "latitude"],
        get_color=["color_r", "color_g", "color_b", 180],
        get_radius=25000,
        pickable=True,
    )

    view = {
        "latitude": float(map_df["latitude"].mean()),
        "longitude": float(map_df["longitude"].mean()),
        "zoom": 2,
        "pitch": 0,
    }

    tooltip = {
        "html": (
            "<b>{callsign}</b><br/>"
            "Country: {origin_country}<br/>"
            "Altitude: {baro_altitude} m<br/>"
            "Velocity: {velocity} m/s<br/>"
            "Anomaly: {is_anomaly}"
        ),
        "style": {"backgroundColor": "#1a1a2e", "color": "white"},
    }

    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip))
else:
    st.info("Waiting for flight data…")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Top 10 Countries by Active Flights
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🌍 Top 10 Countries by Active Flights")

if not flights.empty and "origin_country" in flights.columns:
    top_countries = (
        flights["origin_country"]
        .value_counts()
        .head(10)
        .reset_index()
    )
    top_countries.columns = ["Country", "Flight Count"]

    fig = px.bar(
        top_countries,
        x="Flight Count",
        y="Country",
        orientation="h",
        color="Flight Count",
        color_continuous_scale="Blues",
        title="Top 10 Countries by Active Flights",
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=400)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No country data available yet.")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Anomaly Feed
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🚨 Anomaly Feed (last 20)")

if not anomalies.empty:
    display_cols = [
        "callsign", "origin_country", "baro_altitude", "velocity",
        "vertical_rate", "is_anomaly", "rule_based_anomaly", "processing_time",
    ]
    existing_cols = [c for c in display_cols if c in anomalies.columns]
    feed = anomalies[existing_cols].tail(20).reset_index(drop=True)

    def highlight_anomaly(row):
        """Highlight rows where rule_based_anomaly is True in red."""
        if row.get("rule_based_anomaly", False):
            return ["background-color: #ff4b4b33"] * len(row)
        if row.get("is_anomaly", False):
            return ["background-color: #ff8c0033"] * len(row)
        return [""] * len(row)

    def anomaly_label(row):
        if row.get("rule_based_anomaly", False):
            return "🔴 Rule"
        if row.get("is_anomaly", False):
            return "🟠 ML"
        return "🟢 Normal"

    feed.insert(0, "status", feed.apply(anomaly_label, axis=1))
    st.dataframe(feed, use_container_width=True, height=500)
else:
    st.info("No anomalies detected yet.")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Windowed Trend Chart
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📈 Flight Traffic Over Time")

if not windowed.empty and "window_start" in windowed.columns:
    fig2 = px.line(
        windowed,
        x="window_start",
        y="flight_count",
        title="Flight Count per 2-Minute Window",
        markers=True,
    )
    fig2.update_layout(
        xaxis_title="Window Start",
        yaxis_title="Flight Count",
        height=400,
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Show avg vertical rate trend too
    if "avg_vertical_rate" in windowed.columns:
        fig3 = px.line(
            windowed,
            x="window_start",
            y="avg_vertical_rate",
            title="Average Vertical Rate Over Time",
            markers=True,
            color_discrete_sequence=["#ff6b6b"],
        )
        fig3.update_layout(
            xaxis_title="Window Start",
            yaxis_title="Avg Vertical Rate (m/s)",
            height=350,
        )
        st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Windowed statistics not available yet.")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — Raw Data Explorer
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
with st.expander("📋 Raw Data Explorer (last 50 flights)", expanded=False):
    if not flights.empty:
        search = st.text_input("🔍 Filter by origin country", "")
        filtered = flights.copy()
        if search:
            filtered = filtered[
                filtered["origin_country"].str.contains(search, case=False, na=False)
            ]
        st.dataframe(filtered.tail(50).reset_index(drop=True), use_container_width=True)
    else:
        st.info("No flight data available.")


# ── Auto-refresh (only when enabled in sidebar) ─────────────────────────────
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.cache_data.clear()
    st.rerun()
