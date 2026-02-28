"""
Entry point for spark-submit.
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/streaming_job.py
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from spark.sparkconsumer import main

if __name__ == "__main__":
    main()
