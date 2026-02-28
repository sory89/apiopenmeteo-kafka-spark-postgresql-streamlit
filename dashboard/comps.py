import pandas as pd
import plotly.express as px
import streamlit as st


def render_metric_cards(stats: dict, use_fahrenheit: bool = False):
    if not stats:
        st.info("Waiting for data...")
        return

    cols = st.columns(5)

    with cols[0]:
        st.metric("Cities Tracked", stats.get("city_count", 0))
    with cols[1]:
        if use_fahrenheit:
            st.metric("Avg Temperature", f"{stats.get('avg_temp_f', 'N/A')}°F")
        else:
            st.metric("Avg Temperature", f"{stats.get('avg_temp_c', 'N/A')}°C")
    with cols[2]:
        if use_fahrenheit:
            st.metric("Max Wind Speed", f"{stats.get('max_wind_mph', 'N/A')} mph")
        else:
            st.metric("Max Wind Speed", f"{stats.get('max_wind_kmh', 'N/A')} km/h")
    with cols[3]:
        st.metric("Active Alerts", stats.get("active_alerts", 0))
    with cols[4]:
        st.metric("Data Points", stats.get("data_points", 0))


def render_current_table(df: pd.DataFrame, use_fahrenheit: bool = False):
    if df.empty:
        st.info("No current weather data available.")
        return

    display_cols = ["city", "country", "weather_description"]

    if use_fahrenheit:
        display_cols += ["temperature_f", "apparent_temperature_f", "wind_speed_mph", "wind_gusts_mph"]
        rename_map = {
            "temperature_f":          "Temp (°F)",
            "apparent_temperature_f": "Feels Like (°F)",
            "wind_speed_mph":         "Wind (mph)",
            "wind_gusts_mph":         "Gusts (mph)",
        }
    else:
        display_cols += ["temperature_c", "apparent_temperature_c", "wind_speed_kmh", "wind_gusts_kmh"]
        rename_map = {
            "temperature_c":          "Temp (°C)",
            "apparent_temperature_c": "Feels Like (°C)",
            "wind_speed_kmh":         "Wind (km/h)",
            "wind_gusts_kmh":         "Gusts (km/h)",
        }

    display_cols += ["humidity_pct", "precipitation_mm", "pressure_hpa", "alert_level"]
    rename_map.update({
        "city":                "City",
        "country":             "Country",
        "weather_description": "Condition",
        "humidity_pct":        "Humidity (%)",
        "precipitation_mm":    "Precip (mm)",
        "pressure_hpa":        "Pressure (hPa)",
        "alert_level":         "Alert",
    })

    available_cols = [c for c in display_cols if c in df.columns]
    display_df     = df[available_cols].rename(columns=rename_map)
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_temperature_chart(df: pd.DataFrame, use_fahrenheit: bool = False):
    if df.empty:
        st.info("No history data for temperature chart.")
        return

    temp_col = "temperature_f" if use_fahrenheit else "temperature_c"
    unit     = "°F" if use_fahrenheit else "°C"

    if temp_col not in df.columns:
        st.info("Temperature data not available.")
        return

    fig = px.line(
        df, x="timestamp", y=temp_col, color="city",
        title=f"Temperature Over Time ({unit})",
        labels={"timestamp": "Time", temp_col: f"Temperature ({unit})", "city": "City"},
    )
    fig.update_layout(height=400, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)


def render_comparison_bar(df: pd.DataFrame, use_fahrenheit: bool = False):
    if df.empty:
        st.info("No data for comparison chart.")
        return

    temp_col = "temperature_f" if use_fahrenheit else "temperature_c"
    unit     = "°F" if use_fahrenheit else "°C"

    if temp_col not in df.columns:
        return

    fig = px.bar(
        df, x="city", y=[temp_col, "humidity_pct"],
        title=f"City Comparison: Temperature ({unit}) & Humidity (%)",
        labels={"city": "City", "value": "Value", "variable": "Metric"},
        barmode="group",
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)


def render_wind_chart(df: pd.DataFrame, use_fahrenheit: bool = False):
    if df.empty:
        st.info("No history data for wind chart.")
        return

    wind_col = "wind_speed_mph" if use_fahrenheit else "wind_speed_kmh"
    unit     = "mph" if use_fahrenheit else "km/h"

    if wind_col not in df.columns:
        st.info("Wind data not available.")
        return

    fig = px.line(
        df, x="timestamp", y=wind_col, color="city",
        title=f"Wind Speed Over Time ({unit})",
        labels={"timestamp": "Time", wind_col: f"Wind Speed ({unit})", "city": "City"},
    )
    fig.update_layout(height=400, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)


def render_map(df: pd.DataFrame):
    if df.empty:
        st.info("No data for map.")
        return
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return
    st.map(df[["latitude", "longitude"]])


def render_alerts(alerts_df: pd.DataFrame):
    if alerts_df.empty:
        st.success("No active weather alerts.")
        return

    color_map = {"severe": "red", "warning": "orange", "advisory": "blue"}

    for _, alert in alerts_df.iterrows():
        level   = alert.get("alert_level", "advisory")
        color   = color_map.get(level, "gray")
        city    = alert.get("city", "Unknown")
        message = alert.get("alert_message", "No details")
        ts      = alert.get("timestamp", "")

        with st.expander(f":{color}[{level.upper()}] {city} - {ts}"):
            st.write(message)
