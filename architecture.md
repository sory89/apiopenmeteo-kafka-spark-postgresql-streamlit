# Architecture

```
Open-Meteo API (18 villes)
        ↓
Kafka Producer (poll toutes les 60s)
        ↓
Kafka Topic : raw-weather (Docker)
        ↓
Spark Streaming (foreachBatch, trigger 30s)
        ↓
PostgreSQL (Docker)
        ↓
Streamlit Dashboard (auto-refresh 10s)
```

## Structure

```
streamlitkafkaspark/
├── dashboard/
│   ├── app.py              # Point d'entrée Streamlit
│   ├── components.py       # Re-export composants UI
│   ├── comps.py            # Composants UI (charts, tables, alertes)
│   ├── db.py               # Accès PostgreSQL
│   └── streamlitdash.py    # Dashboard complet
├── producer/
│   ├── config.py           # Villes, Kafka, PG config
│   └── weather_producer.py # Boucle de collecte Kafka
├── spark/
│   ├── sparkconsumer.py    # Consumer Spark + écriture PostgreSQL
│   ├── streaming_job.py    # Entry point spark-submit
│   └── transformations.py  # Transformations & alertes
├── scripts/
│   └── start.sh            # Démarrage complet
├── data/                   # Spark checkpoints (gitignored)
├── docker-compose.yml      # Kafka + Zookeeper + PostgreSQL
└── requirements.txt
```

## Démarrage

```bash
sudo apt install default-jdk -y
pip install -r requirements.txt
chmod +x scripts/start.sh
./scripts/start.sh
```

Dashboard : http://localhost:8501

## PostgreSQL

- Host     : localhost:5432
- Database : weather
- User     : weather_user
- Password : weather_pass

Tables : current_weather · weather_history · weather_alerts
