import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from producer.config import PG_DSN


# Single engine reused across all calls
_engine = create_engine(PG_DSN, pool_pre_ping=True)


def get_current_weather() -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(text("SELECT * FROM current_weather ORDER BY city"), conn)
    except Exception:
        return pd.DataFrame()


def get_weather_history(cities: list[str] | None = None, hours: int = 6) -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            query  = "SELECT * FROM weather_history WHERE timestamp >= NOW() - INTERVAL :interval"
            params = {"interval": f"{hours} hours"}

            if cities:
                query += " AND city = ANY(:cities)"
                params["cities"] = cities

            query += " ORDER BY timestamp ASC"
            return pd.read_sql(text(query), conn, params=params)
    except Exception:
        return pd.DataFrame()


def get_active_alerts() -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(text("""
                SELECT * FROM weather_alerts
                WHERE timestamp >= NOW() - INTERVAL '6 hours'
                ORDER BY timestamp DESC
            """), conn)
    except Exception:
        return pd.DataFrame()


def get_aggregate_stats() -> dict:
    try:
        with _engine.connect() as conn:
            row = conn.execute(text("""
                SELECT
                    COUNT(*)                                                   AS city_count,
                    ROUND(AVG(temperature_c)::numeric, 1)                      AS avg_temp_c,
                    ROUND(AVG(temperature_f)::numeric, 1)                      AS avg_temp_f,
                    ROUND(MAX(wind_speed_kmh)::numeric, 1)                     AS max_wind_kmh,
                    ROUND(MAX(wind_speed_mph)::numeric, 1)                     AS max_wind_mph,
                    SUM(CASE WHEN alert_level != 'normal' THEN 1 ELSE 0 END)   AS active_alerts
                FROM current_weather
            """)).fetchone()

            history_count = conn.execute(
                text("SELECT COUNT(*) FROM weather_history")
            ).fetchone()[0]

        if row is None or row[0] == 0:
            return {}

        return {
            "city_count":    row[0],
            "avg_temp_c":    float(row[1]) if row[1] is not None else None,
            "avg_temp_f":    float(row[2]) if row[2] is not None else None,
            "max_wind_kmh":  float(row[3]) if row[3] is not None else None,
            "max_wind_mph":  float(row[4]) if row[4] is not None else None,
            "active_alerts": row[5] or 0,
            "data_points":   history_count,
        }

    except Exception:
        return {}