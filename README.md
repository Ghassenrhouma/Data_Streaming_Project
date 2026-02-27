# ✈️ OpenSky Real-Time Flight Streaming Pipeline

A complete **real-time data streaming pipeline** that ingests live flight data from the [OpenSky Network API](https://opensky-network.org/), processes it through Apache Kafka and Spark Structured Streaming, detects anomalies using machine learning (Isolation Forest), and visualises everything on an auto-refreshing Streamlit dashboard.

Built as a Master's project in **AI & Big Data**.

---

##  Team

| Name | GitHub | Contributions |
|---|---|---|
| **Ghassen Rhouma** | [@Ghassenrhouma](https://github.com/Ghassenrhouma) | Kafka producer, Spark streaming pipeline, ML model, Streamlit dashboard |
| **Oubeid Allah Jemli** | — | Data processing, ML anomaly detection, system integration & testing |

---

## Architecture

```
┌──────────────┐      ┌──────────────┐      ┌────────────────────────────────┐
│  OpenSky API │─────▶│    Kafka     │─────▶│  Spark Structured Streaming    │
│   (REST)     │      │   Producer   │      │  • Data Cleaning               │
│              │      │              │      │  • Country Aggregations        │
└──────────────┘      └──────┬───────┘      │  • 2-min Windowed Stats        │
                             │              │  • ML Anomaly Detection        │
                     ┌───────▼───────┐      └──────────────┬─────────────────┘
                     │  Kafka Topic  │                     │
                     │  opensky-     │                     ▼
                     │  flights      │      ┌────────────────────────────────┐
                     └───────────────┘      │  JSON Output Files             │
                                            │  • cleaned_flights/            │
                                            │  • windowed_stats/             │
                                            │  • anomalies/                  │
                                            └──────────────┬─────────────────┘
                                                           │
                                                           ▼
                                            ┌────────────────────────────────┐
                                            │  Streamlit Dashboard           │
                                            │  • Live Flight Map             │
                                            │  • KPI Metrics (with deltas)   │
                                            │  • Anomaly Feed                │
                                            │  • Traffic Trend Charts        │
                                            │  • Manual / Auto Refresh       │
                                            └────────────────────────────────┘
```

---

##  Tech Stack

| Technology | Version | Role |
|---|---|---|
| **Apache Kafka** | Latest (Docker) | Message broker — ingests flight data in real time |
| **Apache Spark** (PySpark) | 4.x | Stream processing — cleaning, aggregation, ML scoring |
| **scikit-learn** | Latest | Isolation Forest model for anomaly detection |
| **Streamlit** | Latest | Real-time dashboard with auto-refresh |
| **Plotly** | Latest | Interactive charts (bar, line) |
| **PyDeck** | Latest | Interactive 3-D flight map |
| **Docker** | Latest | Containerised Kafka + Zookeeper |
| **Python** | 3.10+ | All application code |

---

## Project Structure

```
opensky-streaming/
├── docker-compose.yml              # Zookeeper + Kafka (Docker)
├── requirements.txt                # Python dependencies
├── .env.example                    # Template for environment variables
├── credentials.json                # OAuth2 credentials (git-ignored)
├── .gitignore
├── QUICKSTART.md                   # Condensed startup guide
│
├── producer/
│   └── opensky_producer.py         # OpenSky API → Kafka producer (OAuth2)
│
├── spark/
│   └── streaming_pipeline.py       # Spark Structured Streaming pipeline
│
├── ml/
│   ├── train_model.py              # Offline training: IsolationForest + Scaler
│   ├── isolation_forest_model.pkl  # Trained model (git-ignored)
│   └── scaler.pkl                  # Fitted scaler (git-ignored)
│
├── dashboard/
│   └── app.py                      # Streamlit real-time dashboard
│
├── output/                         # Spark output (git-ignored)
│   ├── cleaned_flights/            # Cleaned flight records (JSON)
│   ├── anomalies/                  # ML + rule-based anomaly scores (JSON)
│   └── windowed_stats/             # 2-min window aggregates (JSON)
│
└── README.md
```

---

##  Setup & Execution

> **New here?** Check the [Quick Start Guide](QUICKSTART.md) for a condensed walkthrough.

### Prerequisites

| Requirement | Notes |
|---|---|
| **Python 3.10+** | [python.org](https://www.python.org/downloads/) |
| **Docker Desktop** | For Kafka + Zookeeper — [docker.com](https://www.docker.com/products/docker-desktop/) |
| **JDK 17+** | Required by PySpark 4.x — [Adoptium](https://adoptium.net/) |
| **OpenSky account** | Register at [opensky-network.org](https://opensky-network.org/) and create an OAuth2 API client |
| **Windows only** | `winutils.exe` + `hadoop.dll` (Hadoop 3.3.6) in `C:\hadoop\bin\` — [cdarlint/winutils](https://github.com/cdarlint/winutils) |

---

### Step-by-Step

#### 1. Clone the repository

```bash
git clone https://github.com/Ghassenrhouma/Data_Streaming_Project.git
cd Data_Streaming_Project
```

#### 2. Create a virtual environment and install dependencies

```bash
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS / Linux
pip install -r requirements.txt
```

#### 3. Configure credentials

Copy the environment template:

```powershell
copy .env.example .env
```

Create a `credentials.json` file in the project root with your OpenSky OAuth2 client details:

```json
{
  "clientId": "YOUR_CLIENT_ID",
  "clientSecret": "YOUR_CLIENT_SECRET"
}
```

#### 4. Set environment variables (Windows PowerShell)

```powershell
$env:JAVA_HOME  = "C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH       += ";C:\hadoop\bin"
```

> Adjust `JAVA_HOME` to match your actual JDK 17 installation path.

#### 5. Start Kafka & Zookeeper

```bash
docker-compose up -d
```

Wait ~15 seconds, then create the Kafka topic:

```powershell
docker exec kafka kafka-topics --create --topic opensky-flights `
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### 6. Train the ML model *(one-time)*

```bash
python ml/train_model.py
```

Fetches ~800 live flights from OpenSky and trains the Isolation Forest model.
Produces `ml/isolation_forest_model.pkl` and `ml/scaler.pkl`.

#### 7. Run the pipeline — open three terminals

| Terminal | Command | Purpose |
|---|---|---|
| **T1** | `python producer/opensky_producer.py` | Polls OpenSky API every 10 s → publishes to Kafka |
| **T2** | `python spark/streaming_pipeline.py` | Reads Kafka → cleans, aggregates, scores anomalies → writes JSON |
| **T3** | `streamlit run dashboard/app.py` | Reads JSON output → live Streamlit dashboard |

The dashboard opens at **http://localhost:8501**.

Use **🔄 Refresh Now** in the sidebar to reload data, or enable the **Auto-refresh** toggle for hands-free monitoring (every 15 s).

#### 8. Stop everything

```powershell
# Stop producer & Spark: Ctrl+C in T1 and T2

# Stop Kafka & Zookeeper
docker-compose down
```

---

##  Features

- **Real-time ingestion** — polls OpenSky API every 10 s via OAuth2 client credentials
- **Data cleaning** — drops invalid records, trims whitespace, filters impossible altitudes/velocities
- **Country aggregations** — live count of active flights per origin country
- **Time-windowed stats** — 2-minute tumbling windows tracking flight count & average vertical rate
- **ML anomaly detection** — Isolation Forest scores every flight in each micro-batch
- **Rule-based anomaly flags** — rapid descent (vr < −15 m/s), dangerously low altitude (< 500 m while airborne), extreme speed (> 400 m/s)
- **Live dashboard** — interactive 3-D map, KPI cards with deltas, anomaly feed, trend charts, data explorer
- **Smart refresh** — manual refresh button + optional auto-refresh toggle; data-freshness timestamps show when Spark last wrote output

---

##  ML Anomaly Detection

### Model: Isolation Forest

| Parameter | Value |
|---|---|
| Algorithm | `sklearn.ensemble.IsolationForest` |
| Contamination | 0.05 (5% expected anomalies) |
| Estimators | 100 |
| Scaler | `StandardScaler` |

### Features

| Feature | Description |
|---|---|
| `baro_altitude` | Barometric altitude (m) |
| `velocity` | Ground speed (m/s) |
| `vertical_rate` | Climb/descent rate (m/s) |
| `heading` | Track angle (°) |
| `longitude` | Longitude (°) |
| `latitude` | Latitude (°) |

### What counts as an anomaly?

- **ML-based**: Isolation Forest flags flights with unusual combinations of speed, altitude, position, and descent rate (`is_anomaly = True`)
- **Rule-based**: Hard safety thresholds (`rule_based_anomaly = True`)
  - Vertical rate < −15 m/s (rapid descent)
  - Altitude < 500 m while the aircraft is airborne
  - Velocity > 400 m/s

---

##  Troubleshooting

| Problem | Solution |
|---|---|
| `JAVA_HOME` error | Point it to a JDK 17+ directory (not JRE) |
| `winutils.exe` error (Windows) | Download from [cdarlint/winutils](https://github.com/cdarlint/winutils) → place in `C:\hadoop\bin\` |
| Kafka connection refused | Ensure Docker Desktop is running and `docker-compose up -d` succeeded |
| 401 from OpenSky API | Check `credentials.json` — your OAuth2 client ID/secret must be correct |
| Dashboard shows no data | Wait 20–30 s for the first Spark micro-batch to complete and write output files |
| `spark-submit` not found | Use `python spark/streaming_pipeline.py` instead of `spark-submit` |

---
## GitHub Workflow

| Branch | Purpose |
|---|---|
| `main` | Stable, merged code — final submission |
| `ghassen-rhouma` | Ghassen Rhouma's development branch |
| `oubeid-jemli` | Oubeid Allah Jemli's development branch |

**Process:** Each contributor works on their own branch → opens a Pull Request → peer review → merge to `main`.

---

## 📜 License

This project is for educational purposes (Master's AI & Big Data program).
Not intended for production use.
