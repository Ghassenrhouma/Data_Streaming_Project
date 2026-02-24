"""
Spark Structured Streaming Pipeline — OpenSky Flights
======================================================
Reads flight data from Kafka topic `opensky-flights`, cleans it,
computes country-level aggregations, time-windowed statistics,
and runs ML + rule-based anomaly detection on each micro-batch.

Usage:
    python spark/streaming_pipeline.py
"""

import json
import os
import pathlib
import uuid

import joblib
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Kafka / output configuration ────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "opensky-flights"

OUTPUT_CLEANED  = "./output/cleaned_flights"
OUTPUT_WINDOWED = "./output/windowed_stats"
OUTPUT_ANOMALIES = "./output/anomalies"

# ── ML model paths (trained offline by ml/train_model.py) ───────────────────
SCRIPT_DIR  = pathlib.Path(__file__).resolve().parent.parent  # opensky-streaming/
MODEL_PATH  = SCRIPT_DIR / "ml" / "isolation_forest_model.pkl"
SCALER_PATH = SCRIPT_DIR / "ml" / "scaler.pkl"

# Features the model was trained on (must match ml/train_model.py)
FEATURE_COLS = [
    "baro_altitude", "velocity", "vertical_rate",
    "heading", "longitude", "latitude",
]

# ── JSON schema matching the producer messages ──────────────────────────────
FLIGHT_SCHEMA = StructType([
    StructField("icao24",         StringType(),  True),
    StructField("callsign",       StringType(),  True),
    StructField("origin_country", StringType(),  True),
    StructField("longitude",      DoubleType(),  True),
    StructField("latitude",       DoubleType(),  True),
    StructField("baro_altitude",  DoubleType(),  True),
    StructField("velocity",       DoubleType(),  True),
    StructField("vertical_rate",  DoubleType(),  True),
    StructField("heading",        DoubleType(),  True),
    StructField("on_ground",      BooleanType(), True),
    StructField("timestamp",      LongType(),    True),
])


def create_spark_session() -> SparkSession:
    """Build a local Spark session with the Kafka connector package."""
    spark = (
        SparkSession.builder
        .appName("OpenSkyStreaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        # Reduce noisy Spark logs
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_from_kafka(spark: SparkSession):
    """
    Subscribe to the Kafka topic and parse each message value
    from raw bytes → JSON → DataFrame columns.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # value is binary → cast to string → parse JSON with known schema
    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), FLIGHT_SCHEMA).alias("data"))
        .select("data.*")
    )
    return parsed


# ─────────────────────────────────────────────────────────────────────────────
# 1. DATA CLEANING
# ─────────────────────────────────────────────────────────────────────────────
def clean_flights(df):
    """
    • Drop rows with null icao24, latitude, longitude, or baro_altitude.
    • Remove physically invalid records (altitude ≤ 0 and velocity < 0).
    • Trim whitespace from string fields.
    • Add a processing_time column.
    """
    cleaned = (
        df
        # Drop nulls in critical columns
        .dropna(subset=["icao24", "latitude", "longitude", "baro_altitude"])
        # Filter out invalid altitude / velocity
        .filter(
            (F.col("baro_altitude") > 0) | (F.col("velocity") >= 0)
        )
        # Trim whitespace
        .withColumn("callsign",       F.trim(F.col("callsign")))
        .withColumn("origin_country", F.trim(F.col("origin_country")))
        # Add processing timestamp
        .withColumn("processing_time", F.current_timestamp())
    )
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# 2. COUNTRY-LEVEL AGGREGATIONS  (complete mode → console)
# ─────────────────────────────────────────────────────────────────────────────
def country_aggregations(cleaned):
    """
    Per origin_country:
      - count of active flights
      - average velocity
      - average baro_altitude
    """
    agg = (
        cleaned
        .groupBy("origin_country")
        .agg(
            F.count("icao24").alias("active_flights"),
            F.round(F.avg("velocity"), 2).alias("avg_velocity"),
            F.round(F.avg("baro_altitude"), 2).alias("avg_altitude"),
        )
        .orderBy(F.desc("active_flights"))
    )
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# 3. WINDOWED STATISTICS  (2-minute tumbling window)
# ─────────────────────────────────────────────────────────────────────────────
def windowed_stats(cleaned):
    """
    Convert Unix epoch → timestamp, then compute over 2-min windows:
      - flight count
      - average vertical_rate
    Useful for detecting mass descent / climb trends.
    """
    with_ts = cleaned.withColumn(
        "event_time", F.to_timestamp(F.col("timestamp"))
    )

    windowed = (
        with_ts
        # Use event_time as the watermark column (allow 1-min late data)
        .withWatermark("event_time", "1 minute")
        .groupBy(F.window(F.col("event_time"), "2 minutes"))
        .agg(
            F.count("icao24").alias("flight_count"),
            F.round(F.avg("vertical_rate"), 2).alias("avg_vertical_rate"),
        )
        # Flatten the window struct for cleaner output
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "flight_count",
            "avg_vertical_rate",
        )
    )
    return windowed


# ─────────────────────────────────────────────────────────────────────────────
# 4. ANOMALY DETECTION  (ML + rule-based, applied via foreachBatch)
# ─────────────────────────────────────────────────────────────────────────────

def load_ml_artefacts(spark: SparkSession):
    """
    Load the Isolation Forest model and StandardScaler from disk and
    broadcast them to all Spark workers so they are available inside
    foreachBatch UDFs without repeated serialisation.
    """
    if not MODEL_PATH.is_file() or not SCALER_PATH.is_file():
        print(f"[!] ML model not found at {MODEL_PATH}")
        print("    Run 'python ml/train_model.py' first.  Anomaly detection disabled.")
        return None, None

    model  = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    print(f"[✓] Loaded ML model  from {MODEL_PATH}")
    print(f"[✓] Loaded scaler    from {SCALER_PATH}")

    bc_model  = spark.sparkContext.broadcast(model)
    bc_scaler = spark.sparkContext.broadcast(scaler)
    return bc_model, bc_scaler


def process_anomaly_batch(batch_df: DataFrame, batch_id: int,
                          bc_model, bc_scaler) -> None:
    """
    Called once per micro-batch.  Applies:
      1. ML-based anomaly detection  (Isolation Forest)
      2. Rule-based anomaly flags
    Then writes all scored records to JSON.
    """
    # Use DataFrame.isEmpty() — avoids spawning a Python RDD worker (PySpark 4.x safe)
    if batch_df.isEmpty():
        return

    # ── Collect the micro-batch to the driver (small enough per 10-s window) ─
    # Convert processing_time to string first to avoid Pandas Timestamp → Spark issues
    pdf = (
        batch_df
        .withColumn("processing_time", F.col("processing_time").cast("string"))
        .toPandas()
    )

    # Fill NaN in feature columns with 0 to avoid numpy errors
    pdf[FEATURE_COLS] = pdf[FEATURE_COLS].fillna(0.0)

    # ── 1. ML scoring ────────────────────────────────────────────────────────
    model  = bc_model.value
    scaler = bc_scaler.value

    X = pdf[FEATURE_COLS].values.astype(np.float64)
    X_scaled = scaler.transform(X)

    pdf["anomaly_score"]  = model.decision_function(X_scaled).astype(float)
    pdf["is_anomaly"]     = (model.predict(X_scaled) == -1)

    # ── 2. Rule-based flags ──────────────────────────────────────────────────
    pdf["rule_based_anomaly"] = (
        (pdf["vertical_rate"] < -15)                                    # rapid descent
        | ((pdf["baro_altitude"] < 500) & (pdf["on_ground"] == False))  # very low + airborne
        | (pdf["velocity"] > 400)                                       # unusually fast
    )

    # ── Select output columns ────────────────────────────────────────────────
    out_cols = [
        "icao24", "callsign", "origin_country",
        "latitude", "longitude", "baro_altitude",
        "velocity", "vertical_rate",
        "anomaly_score", "is_anomaly", "rule_based_anomaly",
        "processing_time",
    ]
    result = pdf[out_cols].copy()

    # Ensure correct dtypes for Spark schema inference
    result["anomaly_score"]       = result["anomaly_score"].astype(float)
    result["is_anomaly"]          = result["is_anomaly"].astype(bool)
    result["rule_based_anomaly"]  = result["rule_based_anomaly"].astype(bool)

    # ── Print detected anomalies to console ──────────────────────────────────
    anomalies = result[result["is_anomaly"] | result["rule_based_anomaly"]]
    if not anomalies.empty:
        print(f"\n{'='*70}")
        print(f"  ANOMALIES DETECTED — batch {batch_id}  ({len(anomalies)} flights)")
        print(f"{'='*70}")
        print(anomalies.to_string(index=False))
        print()

    # ── Write all scored records directly from pandas (avoids re-entrant Python
    #    worker deadlock when calling Spark .json() inside foreachBatch) ─────────
    os.makedirs(OUTPUT_ANOMALIES, exist_ok=True)
    out_file = os.path.join(OUTPUT_ANOMALIES, f"batch_{batch_id}_{uuid.uuid4().hex}.json")
    result.to_json(out_file, orient="records", lines=True)


# ─────────────────────────────────────────────────────────────────────────────
# 5. OUTPUT SINKS
# ─────────────────────────────────────────────────────────────────────────────
def start_sinks(cleaned, country_agg, windowed, bc_model, bc_scaler):
    """Start streaming queries (3 original + anomaly detection) and block."""

    # Query 1 — cleaned flights → JSON files (append mode)
    q_cleaned = (
        cleaned.writeStream
        .queryName("cleaned_flights")
        .outputMode("append")
        .format("json")
        .option("path", OUTPUT_CLEANED)
        .option("checkpointLocation", f"{OUTPUT_CLEANED}/_checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[✓] Query 'cleaned_flights' writing to {OUTPUT_CLEANED}/")

    # Query 2 — country aggregations → console (complete mode, for debugging)
    q_country = (
        country_agg.writeStream
        .queryName("country_aggregations")
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 30)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print("[✓] Query 'country_aggregations' writing to console")

    # Query 3 — windowed stats → JSON files (append mode)
    q_windowed = (
        windowed.writeStream
        .queryName("windowed_stats")
        .outputMode("append")
        .format("json")
        .option("path", OUTPUT_WINDOWED)
        .option("checkpointLocation", f"{OUTPUT_WINDOWED}/_checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[✓] Query 'windowed_stats' writing to {OUTPUT_WINDOWED}/")

    # Query 4 — anomaly detection via foreachBatch (ML + rules)
    if bc_model and bc_scaler:
        q_anomaly = (
            cleaned.writeStream
            .queryName("anomaly_detection")
            .outputMode("append")
            .foreachBatch(
                lambda df, bid: process_anomaly_batch(df, bid, bc_model, bc_scaler)
            )
            .trigger(processingTime="10 seconds")
            .option("checkpointLocation", f"{OUTPUT_ANOMALIES}/_checkpoint")
            .start()
        )
        print(f"[✓] Query 'anomaly_detection' writing to {OUTPUT_ANOMALIES}/")
    else:
        print("[!] Anomaly detection query skipped (no model loaded).")

    # Block until any query terminates (or user hits Ctrl+C)
    q_cleaned.awaitTermination()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  OpenSky — Spark Structured Streaming Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    # Load ML artefacts and broadcast to workers
    bc_model, bc_scaler = load_ml_artefacts(spark)

    # Source
    raw_flights = read_from_kafka(spark)

    # Transform
    cleaned  = clean_flights(raw_flights)
    country  = country_aggregations(cleaned)
    windowed = windowed_stats(cleaned)

    # Sink (including anomaly detection)
    start_sinks(cleaned, country, windowed, bc_model, bc_scaler)


if __name__ == "__main__":
    main()
