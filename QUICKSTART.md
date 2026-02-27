# ⚡ Quick Start Guide

Get the OpenSky Real-Time Flight Streaming Pipeline running in **under 10 minutes**.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10+ | [python.org](https://www.python.org/downloads/) |
| Docker Desktop | Latest | [docker.com](https://www.docker.com/products/docker-desktop/) |
| JDK | 17+ | Required by PySpark 4.x — [Adoptium](https://adoptium.net/) |
| OpenSky account | — | Register at [opensky-network.org](https://opensky-network.org/) and create an API client (OAuth2) |

> **Windows only:** Download `winutils.exe` + `hadoop.dll` for Hadoop 3.3.6 from [cdarlint/winutils](https://github.com/cdarlint/winutils) and place them in `C:\hadoop\bin\`.

---

## 1 · Clone & Install

```powershell
git clone https://github.com/Ghassenrhouma/Data_Streaming_Project.git
cd Data_Streaming_Project
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## 2 · Configure Credentials

Copy the template and fill in your OpenSky OAuth2 client ID and secret:

```powershell
copy .env.example .env
```

Then create a `credentials.json` file in the project root:

```json
{
  "clientId": "YOUR_CLIENT_ID",
  "clientSecret": "YOUR_CLIENT_SECRET"
}
```

## 3 · Set Environment Variables

```powershell
$env:JAVA_HOME  = "C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH       += ";C:\hadoop\bin"
```

> Adjust `JAVA_HOME` to match your JDK 17 installation path.

## 4 · Start Kafka

```powershell
docker-compose up -d
```

Wait ~15 seconds for Kafka to be ready, then create the topic:

```powershell
docker exec kafka kafka-topics --create --topic opensky-flights `
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 5 · Train the ML Model (one-time)

```powershell
python ml/train_model.py
```

This fetches ~800 live flights from OpenSky and trains an Isolation Forest model.  
Produces `ml/isolation_forest_model.pkl` and `ml/scaler.pkl`.

## 6 · Launch the Pipeline

Open **three separate terminals** (all with the venv activated and env vars set):

| Terminal | Command | What it does |
|---|---|---|
| **T1** | `python producer/opensky_producer.py` | Polls OpenSky API → publishes to Kafka |
| **T2** | `python spark/streaming_pipeline.py` | Reads Kafka → cleans, aggregates, detects anomalies → writes JSON |
| **T3** | `streamlit run dashboard/app.py` | Reads JSON output → live dashboard |

## 7 · Open the Dashboard

Go to **http://localhost:8501**

- Click **🔄 Refresh Now** in the sidebar to see the latest data
- Toggle **Auto-refresh** for hands-free live monitoring (every 15 s)
- KPI cards show deltas (▲/▼) between refreshes
- File timestamps at the top indicate when Spark last wrote new data

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `spark-submit` not found | Run with `python spark/streaming_pipeline.py` instead |
| `JAVA_HOME` error | Point it to a JDK 17+ directory (not JRE) |
| `winutils.exe` error (Windows) | Download from [cdarlint/winutils](https://github.com/cdarlint/winutils) → `C:\hadoop\bin\` |
| Kafka connection refused | Make sure Docker Desktop is running and `docker-compose up -d` succeeded |
| 401 from OpenSky API | Check `credentials.json` — your OAuth2 client ID/secret must be valid |
| Dashboard shows no data | Wait 20–30 s for the first Spark batch to complete and write output files |

---

## Stopping Everything

```powershell
# Stop producer & Spark: Ctrl+C in their terminals

# Stop Kafka & Zookeeper
docker-compose down
```
