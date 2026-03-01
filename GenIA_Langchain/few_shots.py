few_shots = [
    {
        'Question': "Quelle est la température à Conakry?",
        'SQLQuery': "SELECT city, temperature_c FROM current_weather WHERE city = 'Conakry'",
        'SQLResult': "Result of the SQL query",
        'Answer': "La température à Conakry est de X°C"
    },
    {
        'Question': "Quelle est la ville la plus chaude?",
        'SQLQuery': "SELECT city, temperature_c FROM current_weather ORDER BY temperature_c DESC LIMIT 1",
        'SQLResult': "Result of the SQL query",
        'Answer': "La ville la plus chaude est X avec Y°C"
    },
    {
        'Question': "Quelles villes ont des alertes actives?",
        'SQLQuery': "SELECT city, alert_level FROM current_weather WHERE alert_level != 'normal'",
        'SQLResult': "Result of the SQL query",
        'Answer': "Les villes avec alertes sont X, Y"
    },
    {
        'Question': "Quel est le taux d'humidité à Tokyo?",
        'SQLQuery': "SELECT city, humidity_pct FROM current_weather WHERE city = 'Tokyo'",
        'SQLResult': "Result of the SQL query",
        'Answer': "Le taux d'humidité à Tokyo est de X%"
    },
    {
        'Question': "Quelle est la vitesse du vent à Dubai?",
        'SQLQuery': "SELECT city, wind_speed_kmh FROM current_weather WHERE city = 'Dubai'",
        'SQLResult': "Result of the SQL query",
        'Answer': "La vitesse du vent à Dubai est de X km/h"
    },
]
