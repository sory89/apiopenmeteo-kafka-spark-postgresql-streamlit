"""
Configuration for the weather data producer.
"""

CITIES = [
    {"name": "New York",     "latitude": 40.7128,  "longitude": -74.0060, "country": "US"},
    {"name": "Miami",        "latitude": 25.7617,  "longitude": -80.1918, "country": "US"},
    {"name": "London",       "latitude": 51.5074,  "longitude":  -0.1278, "country": "GB"},
    {"name": "Paris",        "latitude": 48.8566,  "longitude":   2.3522, "country": "FR"},
    {"name": "Tokyo",        "latitude": 35.6762,  "longitude": 139.6503, "country": "JP"},
    {"name": "Sydney",       "latitude": -33.8688, "longitude": 151.2093, "country": "AU"},
    {"name": "Dubai",        "latitude": 25.2048,  "longitude":  55.2708, "country": "AE"},
    {"name": "Sao Paulo",    "latitude": -23.5505, "longitude": -46.6333, "country": "BR"},
    {"name": "Mumbai",       "latitude": 19.0760,  "longitude":  72.8777, "country": "IN"},
    {"name": "New Delhi",    "latitude": 28.6139,  "longitude":  77.2090, "country": "IN"},
    {"name": "Cape Town",    "latitude": -33.9249, "longitude":  18.4241, "country": "ZA"},
    {"name": "Moscow",       "latitude": 55.7558,  "longitude":  37.6173, "country": "RU"},
    {"name": "Toronto",      "latitude": 43.6532,  "longitude": -79.3832, "country": "CA"},
    {"name": "Montreal",     "latitude": 45.5017,  "longitude": -73.5673, "country": "CA"},
    {"name": "Conakry",      "latitude":  9.6412,  "longitude": -13.5784, "country": "GN"},
    {"name": "Niamey",       "latitude": 13.5116,  "longitude": 2.1254,   "country": "NE"},
    {"name": "Johannesburg", "latitude": -26.2041, "longitude":  28.0473, "country": "ZA"},
    {"name": "Rabat",        "latitude": 34.0209,  "longitude":  -6.8416, "country": "MA"},
    {"name": "Riyadh",       "latitude": 24.7136,  "longitude":  46.6753, "country": "SA"},
]

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-weather"

POLL_INTERVAL_SECONDS = 60

# Open-Meteo
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

OPEN_METEO_PARAMS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
    "surface_pressure",
]

# PostgreSQL
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DB       = "weather"
PG_USER     = "weather_user"
PG_PASSWORD = "weather_pass"
PG_DSN      = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
