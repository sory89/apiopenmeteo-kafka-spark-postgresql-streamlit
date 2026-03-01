#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# --- Activate virtualenv ---
if [ -f "$PROJECT_ROOT/env_work/bin/activate" ]; then
  source "$PROJECT_ROOT/env_work/bin/activate"
elif [ -f "$PROJECT_ROOT/../env_work/bin/activate" ]; then
  source "$PROJECT_ROOT/../env_work/bin/activate"
fi

# --- Auto-detect JAVA_HOME ---
if [ -z "$JAVA_HOME" ]; then
  JAVA_BIN="$(readlink -f $(which java) 2>/dev/null || true)"
  if [ -n "$JAVA_BIN" ]; then
    export JAVA_HOME="$(dirname $(dirname $JAVA_BIN))"
    echo "JAVA_HOME=$JAVA_HOME"
  else
    echo "ERROR: Java not found. Run: sudo apt install default-jdk"
    exit 1
  fi
fi

echo "=== Weather Streaming Dashboard (PostgreSQL) ==="
echo ""

mkdir -p data

# 1. Start infrastructure
echo "[1/4] Starting Kafka, Zookeeper & PostgreSQL..."
docker compose up -d

echo "Waiting for Kafka..."
sleep 15

echo "Waiting for PostgreSQL..."
until docker compose exec -T postgres pg_isready -U weather_user -d weather > /dev/null 2>&1; do
  echo "  PostgreSQL not ready, retrying..."
  sleep 3
done
echo "PostgreSQL ready."

# 2. Start producer
echo "[2/4] Starting weather producer..."
python producer/weather_producer.py &
PRODUCER_PID=$!
echo "Producer PID: $PRODUCER_PID"
sleep 5

# 3. Start Spark
echo "[3/4] Starting Spark streaming job..."
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby" \
  spark/streaming_job.py &

SPARK_PID=$!
echo "Spark PID: $SPARK_PID"
sleep 10

# 4. Start Streamlit
echo "[4/4] Starting Streamlit dashboard..."
echo ""
echo "→ http://localhost:8501"
echo "Press Ctrl+C to stop."
echo ""


cleanup() {
  echo "Shutting down..."
  kill $PRODUCER_PID 2>/dev/null || true
  kill $SPARK_PID    2>/dev/null || true
  docker compose down
  echo "Done."
}
trap cleanup EXIT INT TERM

jupyter notebook &

streamlit run dashboard/app.py --server.headless true 
