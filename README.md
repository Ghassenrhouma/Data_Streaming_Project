# OpenSky Real-Time Streaming Pipeline

Real-time flight tracking pipeline built with **Kafka**, **Spark Structured Streaming**, and **Streamlit**.

## Architecture

```
OpenSky REST API  →  Kafka Producer  →  Kafka  →  Spark Streaming  →  Streamlit Dashboard
```

## Project Structure

```
opensky-streaming/
├── docker-compose.yml          # Zookeeper + Kafka
├── requirements.txt            # Python dependencies
├── producer/
│   └── opensky_producer.py     # OpenSky → Kafka producer
├── spark/
│   └── (coming soon)           # Spark Structured Streaming jobs
├── dashboard/
│   └── (coming soon)           # Streamlit dashboard
└── README.md
```

## Quick Start

### 1. Start Kafka & Zookeeper

```bash
docker-compose up -d
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Create the Kafka topic

```bash
docker exec kafka kafka-topics --create --topic opensky-flights --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 4. Run the producer

```bash
# Anonymous access
python producer/opensky_producer.py

# With OpenSky credentials (higher rate limits)
OPENSKY_USER=your_user OPENSKY_PASS=your_pass python producer/opensky_producer.py
```

### 5. Verify messages in Kafka

```bash
docker exec kafka kafka-console-consumer --topic opensky-flights --bootstrap-server localhost:9092 --from-beginning --max-messages 5
```

## Environment Variables

| Variable       | Default          | Description                        |
|----------------|------------------|------------------------------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server address     |
| `OPENSKY_USER` | *(none)*         | OpenSky username (optional)        |
| `OPENSKY_PASS` | *(none)*         | OpenSky password (optional)        |

## Tech Stack

- **Apache Kafka** – message broker
- **Apache Spark** – stream processing (Step 2)
- **Streamlit** – real-time dashboard (Step 3)
- **scikit-learn** – ML models (Step 4)
- **Docker** – containerized infrastructure
