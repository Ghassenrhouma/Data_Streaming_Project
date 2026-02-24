# ✈️ OpenSky Real-Time Flight Streaming Pipeline

A complete **real-time data streaming pipeline** that ingests live flight data from the [OpenSky Network API](https://opensky-network.org/), processes it through Apache Kafka and Spark Structured Streaming, detects anomalies using machine learning (Isolation Forest), and visualises everything on an auto-refreshing Streamlit dashboard.

Built as a Master's project in **AI & Big Data**.

---

## 📐 Architecture

```
┌──────────────┐      ┌──────────────┐      ┌────────────────────────────────┐
│  OpenSky API │─────▶│ Kafka        │─────▶│  Spark Structured Streaming    │
│  (REST)      │      │ Producer     │      │  • Data Cleaning               │
│              │      │              │      │  • Country Aggregations        │
└──────────────┘      └──────┬───────┘      │  • 2-min Windowed Stats        │
                             │              │  • ML Anomaly Detection        │
                     ┌───────▼───────┐      └──────────────┬─────────────────┘
                     │  Kafka Topic  │                     │
                     │ opensky-      │                     ▼
                     │ flights       │      ┌────────────────────────────────┐
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

## 🛠️ Tech Stack

| Technology | Role |
|---|---|
| **Apache Kafka** | Message broker — ingests flight data in real time |
| **Apache Spark** (PySpark 4.1) | Stream processing — cleaning, aggregation, ML scoring |
| **scikit-learn** | Isolation Forest model for anomaly detection |
| **Streamlit** | Real-time dashboard with auto-refresh |
| **Plotly** | Interactive charts (bar, line) |
| **PyDeck** | Interactive flight map |
| **Docker** | Containerised Kafka + Zookeeper |
| **Python 3.10+** | All application code |

---

## 📁 Project Structure

```
opensky-streaming/
├── docker-compose.yml              # Zookeeper + Kafka (Docker)
├── requirements.txt                # Python dependencies
├── credentials.json                # OAuth2 credentials (git-ignored)
├── .gitignore
├── .env.example                    # Template for environment variables
├── QUICKSTART.md                   # Quick startup guide
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

## 🚀 Setup & Execution

> **👉 New here?** Check the [Quick Start Guide](QUICKSTART.md) for a streamlined walkthrough.

### Prerequisites

- **Python 3.10+**
- **Docker Desktop** (for Kafka + Zookeeper)
- **JDK 17** (required by PySpark 4.x)
- **Windows only**: `winutils.exe` + `hadoop.dll` in `C:\hadoop\bin\` ([download](https://github.com/cdarlint/winutils))

### Step-by-step

```bash
# 1. Clone the repository
git clone https://github.com/Ghassenrhouma/Data_Streaming_Project.git
cd Data_Streaming_Project

# 2. Create virtual environment & install dependencies
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt

# 3. Set environment variables (Windows PowerShell)
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH += ";C:\hadoop\bin"

# 4. Start Kafka & Zookeeper
docker-compose up -d

# 5. Create the Kafka topic
docker exec kafka kafka-topics --create --topic opensky-flights ^
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 6. Train the ML model (one-time — collects ~800 flights from OpenSky)
python ml/train_model.py

# 7. Start the Kafka producer  (Terminal 1 — runs continuously)
python producer/opensky_producer.py

# 8. Start the Spark Streaming pipeline  (Terminal 2 — runs continuously)
python spark/streaming_pipeline.py

# 9. Launch the Streamlit dashboard  (Terminal 3)
streamlit run dashboard/app.py
```

The dashboard opens at **http://localhost:8501**.  
Use the **🔄 Refresh Now** button in the sidebar to reload data, or enable the **Auto-refresh** toggle for hands-free monitoring (every 15 s).

---

## ✨ Features

- **Real-time ingestion** — polls OpenSky API every 10 s via OAuth2 client credentials
- **Data cleaning** — drops invalid records, trims whitespace, filters impossible altitudes/velocities
- **Country aggregations** — live count of active flights per country
- **Time-windowed stats** — 2-minute tumbling windows tracking flight count & average vertical rate
- **ML anomaly detection** — Isolation Forest scores every flight in each micro-batch
- **Rule-based anomaly flags** — rapid descent (vr < −15), dangerously low altitude (< 500 m while airborne), extreme speed (> 400 m/s)
- **Live dashboard** — interactive map, KPI cards with deltas, anomaly feed, trend charts, data explorer
- **Smart refresh** — manual refresh button + optional auto-refresh toggle (sidebar); data-freshness timestamps show when Spark last wrote output

---

## 🤖 ML Anomaly Detection

### Model: Isolation Forest

| Parameter | Value |
|---|---|
| Algorithm | `sklearn.ensemble.IsolationForest` |
| Contamination | 0.05 (5% expected anomalies) |
| Estimators | 100 |
| Scaler | `StandardScaler` |

### Features used

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
- **Rule-based**: Hard thresholds for safety-critical scenarios (`rule_based_anomaly = True`)
  - Vertical rate < −15 m/s (rapid descent)
  - Altitude < 500 m while airborne
  - Velocity > 400 m/s

---

## 🔀 GitHub Workflow

| Branch | Purpose |
|---|---|
| `main` | Stable, merged code — final submission |
| `ghassen-rhouma` | Ghassen's development branch |
| *(team branches)* | One branch per team member |

**Process:** Each contributor works on their own branch → opens a Pull Request → review → merge to `main`.

---

## 👥 Team

| Name | GitHub Username | Contributions |
|---|---|---|
| Ghassen Rhouma | [@Ghassenrhouma](https://github.com/Ghassenrhouma) | Full pipeline: Kafka producer, Spark streaming, ML model, Dashboard |

---

## 📜 License

This project is for educational purposes (Master's AI & Big Data program).
