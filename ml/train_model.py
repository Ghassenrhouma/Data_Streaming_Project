"""
Offline Model Training — Isolation Forest for Flight Anomaly Detection
=======================================================================
1. Fetches ~500-1000 live flight records from the OpenSky Network API
   to build a "normal behaviour" baseline.
2. Trains a StandardScaler + Isolation Forest (contamination=0.05).
3. Saves both artefacts to ml/isolation_forest_model.pkl and ml/scaler.pkl.

Usage:
    python ml/train_model.py
"""

import json
import logging
import os
import pathlib
import sys
import time

import joblib
import numpy as np
import pandas as pd
import requests
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ── Configuration ────────────────────────────────────────────────────────────
OPENSKY_URL = "https://opensky-network.org/api/states/all"
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)

# Features used by the anomaly detection model
FEATURE_COLS = [
    "baro_altitude",
    "velocity",
    "vertical_rate",
    "heading",
    "longitude",
    "latitude",
]

# Number of API calls to accumulate enough samples
TARGET_SAMPLES = 800        # aim for ~800 unique flights
MAX_API_CALLS  = 5          # at most 5 requests (10 s apart)
POLL_INTERVAL  = 12         # seconds between calls

# Model hyper-parameters
CONTAMINATION = 0.05        # 5 % assumed outlier ratio
RANDOM_STATE  = 42

# Output paths (relative to project root)
SCRIPT_DIR   = pathlib.Path(__file__).resolve().parent        # ml/
PROJECT_ROOT = SCRIPT_DIR.parent                               # opensky-streaming/
MODEL_PATH   = SCRIPT_DIR / "isolation_forest_model.pkl"
SCALER_PATH  = SCRIPT_DIR / "scaler.pkl"

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("train-model")


# ── OAuth2 credentials (reuse the same credentials.json as the producer) ────
def _get_oauth2_token() -> str | None:
    """Obtain a Bearer token using credentials.json if present."""
    cred_path = PROJECT_ROOT / "credentials.json"
    if not cred_path.is_file():
        return None

    with open(cred_path, encoding="utf-8") as f:
        creds = json.load(f)

    client_id = creds.get("clientId")
    client_secret = creds.get("clientSecret")
    if not client_id or not client_secret:
        return None

    try:
        resp = requests.post(
            OPENSKY_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=10,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        log.info("Authenticated via OAuth2 (credentials.json).")
        return token
    except requests.RequestException as exc:
        log.warning("OAuth2 token request failed: %s — falling back to anonymous.", exc)
        return None


# ── Data collection ─────────────────────────────────────────────────────────
def fetch_flights(token: str | None) -> list[dict]:
    """Call the OpenSky API once and return a list of flight dicts."""
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.get(OPENSKY_URL, headers=headers, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("API request failed: %s", exc)
        return []

    states = resp.json().get("states", [])
    flights = []
    for s in states:
        flights.append({
            "icao24":         s[0],
            "callsign":       (s[1] or "").strip(),
            "origin_country": s[2],
            "longitude":      s[5],
            "latitude":       s[6],
            "baro_altitude":  s[7],
            "velocity":       s[9],
            "vertical_rate":  s[11],
            "heading":        s[10],
            "on_ground":      s[8],
            "timestamp":      s[3],
        })
    return flights


def collect_training_data() -> pd.DataFrame:
    """
    Accumulate flight records across multiple API calls until we reach
    TARGET_SAMPLES unique flights (by icao24).
    """
    token = _get_oauth2_token()
    all_flights: list[dict] = []

    for i in range(1, MAX_API_CALLS + 1):
        log.info("API call %d / %d …", i, MAX_API_CALLS)
        batch = fetch_flights(token)
        all_flights.extend(batch)
        unique = len({f["icao24"] for f in all_flights})
        log.info("  → %d records total (%d unique icao24).", len(all_flights), unique)

        if unique >= TARGET_SAMPLES:
            break
        if i < MAX_API_CALLS:
            log.info("  Waiting %d s before next call …", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)

    df = pd.DataFrame(all_flights)
    log.info("Collected %d raw records.", len(df))
    return df


# ── Preprocessing ───────────────────────────────────────────────────────────
def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the DataFrame so it is ready for model training."""
    before = len(df)

    # Keep only airborne flights with valid feature values
    df = df[df["on_ground"] == False].copy()
    df = df.dropna(subset=FEATURE_COLS)
    df = df[df["baro_altitude"] > 0]
    df = df[df["velocity"] >= 0]

    # De-duplicate by icao24 (keep latest record per aircraft)
    df = df.sort_values("timestamp", ascending=False).drop_duplicates(subset="icao24")

    log.info("After cleaning: %d → %d samples.", before, len(df))
    return df


# ── Training ────────────────────────────────────────────────────────────────
def train(df: pd.DataFrame) -> None:
    """Train StandardScaler + IsolationForest and persist both."""
    X = df[FEATURE_COLS].values.astype(np.float64)

    # 1. Fit scaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    log.info("Scaler fitted.  Feature means: %s", dict(zip(FEATURE_COLS, scaler.mean_.round(2))))

    # 2. Fit Isolation Forest
    model = IsolationForest(
        contamination=CONTAMINATION,
        n_estimators=100,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    # Prediction sanity check on training set
    preds = model.predict(X_scaled)
    n_anomalies = (preds == -1).sum()
    log.info(
        "Training complete.  %d samples, %d flagged as anomalies (%.1f%%).",
        len(X), n_anomalies, 100 * n_anomalies / len(X),
    )

    # 3. Save artefacts
    SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    log.info("Model saved  → %s", MODEL_PATH)
    log.info("Scaler saved → %s", SCALER_PATH)

    # 4. Summary
    print("\n" + "=" * 60)
    print("  Training Summary")
    print("=" * 60)
    print(f"  Samples:        {len(X)}")
    print(f"  Features:       {FEATURE_COLS}")
    print(f"  Contamination:  {CONTAMINATION}")
    print(f"  Anomalies found in training set: {n_anomalies} ({100*n_anomalies/len(X):.1f}%)")
    print(f"\n  Feature means:")
    for feat, mean_val in zip(FEATURE_COLS, scaler.mean_):
        print(f"    {feat:20s}  {mean_val:>12.2f}")
    print(f"\n  Feature std devs:")
    for feat, std_val in zip(FEATURE_COLS, scaler.scale_):
        print(f"    {feat:20s}  {std_val:>12.2f}")
    print(f"\n  Model params:   {model.get_params()}")
    print(f"  Model file:     {MODEL_PATH}")
    print(f"  Scaler file:    {SCALER_PATH}")
    print("=" * 60)


# ── Entry point ─────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Isolation Forest — Offline Training")
    print("=" * 60)

    df = collect_training_data()

    if df.empty:
        log.error("No data collected. Check your API credentials / network.")
        sys.exit(1)

    df = preprocess(df)

    if len(df) < 50:
        log.error("Only %d samples after cleaning — not enough to train.", len(df))
        sys.exit(1)

    train(df)


if __name__ == "__main__":
    main()
