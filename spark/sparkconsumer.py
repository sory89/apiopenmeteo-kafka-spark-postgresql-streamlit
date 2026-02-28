import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from spark.transformations import transform_record
from producer.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "spark-checkpoints")

KAFKA_SCHEMA = StructType([
    StructField("city",                   StringType(), True),
    StructField("country",                StringType(), True),
    StructField("latitude",               DoubleType(), True),
    StructField("longitude",              DoubleType(), True),
    StructField("timestamp",              StringType(), True),
    StructField("temperature_c",          DoubleType(), True),
    StructField("humidity_pct",           DoubleType(), True),
    StructField("apparent_temperature_c", DoubleType(), True),
    StructField("precipitation_mm",       DoubleType(), True),
    StructField("wind_speed_kmh",         DoubleType(), True),
    StructField("wind_gusts_kmh",         DoubleType(), True),
    StructField("weather_code",           IntegerType(),True),
    StructField("pressure_hpa",           DoubleType(), True),
])


def _get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def init_db():
    """Create PostgreSQL tables and indexes if they don't exist."""
    conn = _get_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS current_weather (
            city                   TEXT PRIMARY KEY,
            country                TEXT,
            latitude               DOUBLE PRECISION,
            longitude              DOUBLE PRECISION,
            timestamp              TIMESTAMPTZ,
            temperature_c          DOUBLE PRECISION,
            temperature_f          DOUBLE PRECISION,
            humidity_pct           DOUBLE PRECISION,
            apparent_temperature_c DOUBLE PRECISION,
            apparent_temperature_f DOUBLE PRECISION,
            precipitation_mm       DOUBLE PRECISION,
            wind_speed_kmh         DOUBLE PRECISION,
            wind_speed_mph         DOUBLE PRECISION,
            wind_gusts_kmh         DOUBLE PRECISION,
            wind_gusts_mph         DOUBLE PRECISION,
            weather_code           INTEGER,
            weather_description    TEXT,
            pressure_hpa           DOUBLE PRECISION,
            alert_level            TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_history (
            id                     SERIAL PRIMARY KEY,
            city                   TEXT,
            country                TEXT,
            latitude               DOUBLE PRECISION,
            longitude              DOUBLE PRECISION,
            timestamp              TIMESTAMPTZ,
            temperature_c          DOUBLE PRECISION,
            temperature_f          DOUBLE PRECISION,
            humidity_pct           DOUBLE PRECISION,
            apparent_temperature_c DOUBLE PRECISION,
            apparent_temperature_f DOUBLE PRECISION,
            precipitation_mm       DOUBLE PRECISION,
            wind_speed_kmh         DOUBLE PRECISION,
            wind_speed_mph         DOUBLE PRECISION,
            wind_gusts_kmh         DOUBLE PRECISION,
            wind_gusts_mph         DOUBLE PRECISION,
            weather_code           INTEGER,
            weather_description    TEXT,
            pressure_hpa           DOUBLE PRECISION,
            alert_level            TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_alerts (
            id            SERIAL PRIMARY KEY,
            city          TEXT,
            timestamp     TIMESTAMPTZ,
            alert_level   TEXT,
            alert_message TEXT
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_history_city_ts ON weather_history(city, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_city_ts  ON weather_alerts(city, timestamp)")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("PostgreSQL tables initialized.")


def cleanup_old_data(cur):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    cur.execute("DELETE FROM weather_history WHERE timestamp < %s", (cutoff,))
    cur.execute("DELETE FROM weather_alerts  WHERE timestamp < %s", (cutoff,))


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    logger.info("Processing batch %d with %d rows", batch_id, len(rows))

    conn = _get_conn()
    cur  = conn.cursor()

    try:
        for row in rows:
            t = transform_record(row.asDict())

            vals = (
                t["city"], t["country"], t["latitude"], t["longitude"], t["timestamp"],
                t["temperature_c"], t["temperature_f"], t["humidity_pct"],
                t["apparent_temperature_c"], t["apparent_temperature_f"],
                t["precipitation_mm"], t["wind_speed_kmh"], t["wind_speed_mph"],
                t["wind_gusts_kmh"], t["wind_gusts_mph"],
                t["weather_code"], t["weather_description"],
                t["pressure_hpa"], t["alert_level"],
            )

            cur.execute("""
                INSERT INTO current_weather (
                    city, country, latitude, longitude, timestamp,
                    temperature_c, temperature_f, humidity_pct,
                    apparent_temperature_c, apparent_temperature_f,
                    precipitation_mm, wind_speed_kmh, wind_speed_mph,
                    wind_gusts_kmh, wind_gusts_mph,
                    weather_code, weather_description,
                    pressure_hpa, alert_level
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (city) DO UPDATE SET
                    country                = EXCLUDED.country,
                    latitude               = EXCLUDED.latitude,
                    longitude              = EXCLUDED.longitude,
                    timestamp              = EXCLUDED.timestamp,
                    temperature_c          = EXCLUDED.temperature_c,
                    temperature_f          = EXCLUDED.temperature_f,
                    humidity_pct           = EXCLUDED.humidity_pct,
                    apparent_temperature_c = EXCLUDED.apparent_temperature_c,
                    apparent_temperature_f = EXCLUDED.apparent_temperature_f,
                    precipitation_mm       = EXCLUDED.precipitation_mm,
                    wind_speed_kmh         = EXCLUDED.wind_speed_kmh,
                    wind_speed_mph         = EXCLUDED.wind_speed_mph,
                    wind_gusts_kmh         = EXCLUDED.wind_gusts_kmh,
                    wind_gusts_mph         = EXCLUDED.wind_gusts_mph,
                    weather_code           = EXCLUDED.weather_code,
                    weather_description    = EXCLUDED.weather_description,
                    pressure_hpa           = EXCLUDED.pressure_hpa,
                    alert_level            = EXCLUDED.alert_level
            """, vals)

            cur.execute("""
                INSERT INTO weather_history (
                    city, country, latitude, longitude, timestamp,
                    temperature_c, temperature_f, humidity_pct,
                    apparent_temperature_c, apparent_temperature_f,
                    precipitation_mm, wind_speed_kmh, wind_speed_mph,
                    wind_gusts_kmh, wind_gusts_mph,
                    weather_code, weather_description,
                    pressure_hpa, alert_level
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, vals)

            if t["alert_level"] != "normal" and t.get("alert_message"):
                cur.execute("""
                    INSERT INTO weather_alerts (city, timestamp, alert_level, alert_message)
                    VALUES (%s, %s, %s, %s)
                """, (t["city"], t["timestamp"], t["alert_level"], t["alert_message"]))

        cleanup_old_data(cur)
        conn.commit()
        logger.info("Batch %d written to PostgreSQL successfully", batch_id)

    except Exception as e:
        conn.rollback()
        logger.error("Error writing batch %d: %s", batch_id, e)
        raise

    finally:
        cur.close()
        conn.close()


def main():
    init_db()

    spark = (
        SparkSession.builder
        .appName("WeatherStreaming")
        .master("local[*]")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "raw-weather")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), KAFKA_SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
