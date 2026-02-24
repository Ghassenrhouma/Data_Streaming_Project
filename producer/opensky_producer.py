"""
OpenSky Network → Kafka Producer
=================================
Polls the OpenSky REST API every 10 seconds and publishes each flight
state vector as a JSON message to the Kafka topic `opensky-flights`.

Authentication:
    1. credentials.json  (OAuth2 clientId / clientSecret → Bearer token)
    2. Environment variables  OPENSKY_USER / OPENSKY_PASS (Basic Auth)
    3. Anonymous access (lowest rate limits)

Other environment variables:
    KAFKA_BROKER  – Kafka bootstrap server (default: localhost:9092)
"""

import json
import logging
import os
import pathlib
import sys
import time

import requests
from kafka import KafkaProducer

# ── Configuration ────────────────────────────────────────────────────────────
OPENSKY_URL = "https://opensky-network.org/api/states/all"
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "opensky-flights"
POLL_INTERVAL = 10  # seconds

# ── Credentials loading ─────────────────────────────────────────────────────

def _load_oauth2_credentials() -> tuple[str | None, str | None]:
    """Load clientId / clientSecret from credentials.json if available."""
    search_paths = [
        pathlib.Path(__file__).resolve().parent.parent / "credentials.json",
        pathlib.Path(__file__).resolve().parent / "credentials.json",
    ]
    for cred_path in search_paths:
        if cred_path.is_file():
            with open(cred_path, encoding="utf-8") as f:
                creds = json.load(f)
            client_id = creds.get("clientId")
            client_secret = creds.get("clientSecret")
            if client_id and client_secret:
                return client_id, client_secret
    return None, None


def _load_basic_credentials() -> tuple[str | None, str | None]:
    """Fall back to env-var based username/password."""
    user = os.getenv("OPENSKY_USER")
    pwd = os.getenv("OPENSKY_PASS")
    if user and pwd:
        return user, pwd
    return None, None


# OAuth2 client credentials (preferred)
CLIENT_ID, CLIENT_SECRET = _load_oauth2_credentials()
# Basic-auth fallback
OPENSKY_USER, OPENSKY_PASS = _load_basic_credentials()

# Cached Bearer token and its expiry
_access_token: str | None = None
_token_expires_at: float = 0.0


def _get_access_token() -> str | None:
    """
    Obtain (or refresh) an OAuth2 access token using the client-credentials
    grant.  Returns None if OAuth2 credentials are not configured.
    """
    global _access_token, _token_expires_at

    if not CLIENT_ID or not CLIENT_SECRET:
        return None

    # Re-use cached token if still valid (with 30 s safety margin)
    if _access_token and time.time() < _token_expires_at - 30:
        return _access_token

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    try:
        resp = requests.post(OPENSKY_TOKEN_URL, data=payload, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("OAuth2 token request failed: %s", exc)
        return None

    token_data = resp.json()
    _access_token = token_data["access_token"]
    _token_expires_at = time.time() + token_data.get("expires_in", 300)
    log.info("Obtained OAuth2 access token (expires in %ds).", token_data.get("expires_in", 0))
    return _access_token

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("opensky-producer")


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_opensky_states() -> list[dict] | None:
    """
    Call the OpenSky Network REST API and return a list of parsed
    flight-state dictionaries.  Returns None on failure.
    """
    headers = {}
    auth = None

    # Prefer OAuth2 Bearer token; fall back to Basic Auth; then anonymous
    token = _get_access_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif OPENSKY_USER and OPENSKY_PASS:
        auth = (OPENSKY_USER, OPENSKY_PASS)

    try:
        response = requests.get(OPENSKY_URL, headers=headers, auth=auth, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        log.error("OpenSky API request failed: %s", exc)
        return None

    data = response.json()
    states = data.get("states")
    if not states:
        log.warning("No state vectors returned by OpenSky.")
        return []

    # Map each state vector (list) to a clean JSON object
    flights = []
    for s in states:
        flight = {
            "icao24":          s[0],
            "callsign":        s[1].strip() if s[1] else None,
            "origin_country":  s[2],
            "longitude":       s[5],
            "latitude":        s[6],
            "baro_altitude":   s[7],
            "velocity":        s[9],
            "vertical_rate":   s[11],
            "heading":         s[10],
            "on_ground":       s[8],
            "timestamp":       s[3],
        }
        flights.append(flight)

    return flights


def run() -> None:
    """Main loop: fetch → publish → sleep → repeat."""
    log.info("Connecting to Kafka at %s …", KAFKA_BROKER)
    producer = create_producer()
    log.info("Kafka producer ready.  Topic: %s", KAFKA_TOPIC)

    while True:
        log.info("Fetching flight data from OpenSky …")
        flights = fetch_opensky_states()

        if flights is None:
            log.warning("Skipping this cycle (API error).")
        elif len(flights) == 0:
            log.info("No flights to send.")
        else:
            for flight in flights:
                producer.send(KAFKA_TOPIC, value=flight)
            producer.flush()
            log.info("✓ Sent %d flights to Kafka.", len(flights))

        log.info("Sleeping %d s …", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Producer stopped by user.")
        sys.exit(0)
