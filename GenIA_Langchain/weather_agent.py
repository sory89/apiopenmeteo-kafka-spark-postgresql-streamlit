import os
from dotenv import load_dotenv
load_dotenv()

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.utilities import SQLDatabase
from langchain_core.example_selectors import SemanticSimilarityExampleSelector
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_core.prompts import FewShotPromptTemplate, PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.tools import StructuredTool
from langgraph.prebuilt import create_react_agent

from few_shots import few_shots

# ─── PROMPTS ─────────────────────────────────────────────────────────
PROMPT_SUFFIX = """Only use the following tables:
{table_info}

Question: {input}"""

_postgres_prompt = """You are a PostgreSQL expert. Given an input question, first create a syntactically correct PostgreSQL query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per PostgreSQL.
Never query for all columns from a table. You must query only the columns that are needed to answer the question.
Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use NOW() instead of date('now') for current date/time queries.
Pay attention to use ROUND(value::numeric, 2) for rounding in PostgreSQL.

Use the following format:
Question: Question here
SQLQuery: Query to run with no pre-amble
SQLResult: Result of the SQLQuery
Answer: Final answer here
No pre-amble.
"""


def build_agent():
    # ─── PostgreSQL ───────────────────────────────────────────────
    db = SQLDatabase.from_uri(
        "postgresql+psycopg2://weather_user:weather_pass@localhost:5432/weather",
        sample_rows_in_table_info=3
    )

    # ─── LLM Gemini ───────────────────────────────────────────────
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=os.environ["GOOGLE_API_KEY"],
        temperature=0.1
    )

    # ─── Embeddings + VectorStore ─────────────────────────────────
    embeddings = HuggingFaceEmbeddings(model_name='sentence-transformers/all-MiniLM-L6-v2')
    to_vectorize = [" ".join(str(v) for v in ex.values()) for ex in few_shots]
    vectorstore = Chroma.from_texts(to_vectorize, embeddings, metadatas=few_shots)
    example_selector = SemanticSimilarityExampleSelector(vectorstore=vectorstore, k=2)

    example_prompt = PromptTemplate(
        input_variables=["Question", "SQLQuery", "SQLResult", "Answer"],
        template="\nQuestion: {Question}\nSQLQuery: {SQLQuery}\nSQLResult: {SQLResult}\nAnswer: {Answer}",
    )
    few_shot_prompt = FewShotPromptTemplate(
        example_selector=example_selector,
        example_prompt=example_prompt,
        prefix=_postgres_prompt,
        suffix=PROMPT_SUFFIX,
        input_variables=["input", "table_info", "top_k"],
    )

    # ─── FONCTIONS OUTILS ─────────────────────────────────────────

    def query_weather_db(question: str) -> str:
        """Interroge la base météo PostgreSQL pour une question sur une ville spécifique."""
        prompt_input = {
            "input": question,
            "table_info": db.get_table_info(),
            "top_k": "5",
        }
        sql_raw = (few_shot_prompt | llm | StrOutputParser()).invoke(prompt_input)
        sql_raw = sql_raw.strip().replace("```sql", "").replace("```", "").strip()
        if "SQLQuery:" in sql_raw:
            sql_query = sql_raw.split("SQLQuery:")[-1].strip()
        else:
            sql_query = sql_raw
        if "SQLResult:" in sql_query:
            sql_query = sql_query.split("SQLResult:")[0].strip()
        try:
            return str(db.run(sql_query))
        except Exception as e:
            return f"Erreur SQL: {e}"

    def check_alerts(question: str) -> str:
        """Vérifie les alertes météo actives dans toutes les villes."""
        try:
            result = db.run("""
                SELECT city, alert_level, weather_description
                FROM current_weather
                WHERE alert_level != 'normal'
                ORDER BY alert_level DESC LIMIT 10
            """)
            return str(result) if result else "Aucune alerte active."
        except Exception as e:
            return f"Erreur: {e}"

    def get_hottest_cities(question: str) -> str:
        """Retourne le classement des 5 villes les plus chaudes."""
        try:
            return str(db.run("""
                SELECT city, temperature_c, weather_description
                FROM current_weather ORDER BY temperature_c DESC LIMIT 5
            """))
        except Exception as e:
            return f"Erreur: {e}"

    def get_coldest_cities(question: str) -> str:
        """Retourne le classement des 5 villes les plus froides."""
        try:
            return str(db.run("""
                SELECT city, temperature_c, weather_description
                FROM current_weather ORDER BY temperature_c ASC LIMIT 5
            """))
        except Exception as e:
            return f"Erreur: {e}"

    def get_global_stats(question: str) -> str:
        """Statistiques météo globales : moyennes, min, max, alertes."""
        try:
            return str(db.run("""
                SELECT
                    COUNT(*) as nb_villes,
                    ROUND(AVG(temperature_c)::numeric,1) as temp_moyenne,
                    ROUND(MIN(temperature_c)::numeric,1) as temp_min,
                    ROUND(MAX(temperature_c)::numeric,1) as temp_max,
                    ROUND(AVG(humidity_pct)::numeric,1) as humidite_moyenne,
                    ROUND(AVG(wind_speed_kmh)::numeric,1) as vent_moyen,
                    COUNT(CASE WHEN alert_level != 'normal' THEN 1 END) as nb_alertes
                FROM current_weather
            """))
        except Exception as e:
            return f"Erreur: {e}"

    def get_weather_history(question: str) -> str:
        """Historique météo des dernières 6 heures."""
        try:
            return str(db.run("""
                SELECT city, temperature_c, humidity_pct, timestamp
                FROM weather_history
                WHERE timestamp >= NOW() - INTERVAL '6 hours'
                ORDER BY timestamp DESC LIMIT 20
            """))
        except Exception as e:
            return f"Erreur: {e}"



    def get_most_humid_cities(question: str) -> str:
        """Retourne les villes avec le taux d humidité le plus élevé."""
        try:
            return str(db.run("""
                SELECT city, humidity_pct, weather_description
                FROM current_weather ORDER BY humidity_pct DESC LIMIT 5
            """))
        except Exception as e:
            return f"Erreur: {e}"

    def get_most_precipitation(question: str) -> str:
        """Retourne les villes avec le plus de précipitations."""
        try:
            return str(db.run("""
                SELECT city, precipitation_mm, weather_description
                FROM current_weather
                WHERE precipitation_mm > 0
                ORDER BY precipitation_mm DESC LIMIT 5
            """))
        except Exception as e:
            return f"Erreur: {e}"

    def get_windiest_cities(question: str) -> str:
        """Retourne les villes avec les vents les plus forts."""
        try:
            return str(db.run("""
                SELECT city, wind_speed_kmh, wind_gusts_kmh
                FROM current_weather ORDER BY wind_speed_kmh DESC LIMIT 5
            """))
        except Exception as e:
            return f"Erreur: {e}"

    # ─── DÉFINITION DES OUTILS ────────────────────────────────────
    tools = [
        StructuredTool.from_function(
            func=query_weather_db,
            name="WeatherDatabase",
            description="Utile pour TOUTE question météo : vent, température, humidité, pression, précipitations, comparaison de villes, classement par vent, ville la plus venteuse, conditions météo générales. Input: question en français."
        ),
        StructuredTool.from_function(
            func=check_alerts,
            name="WeatherAlerts",
            description="Utile pour vérifier les alertes météo actives, conditions dangereuses, risques. Input: question sur les alertes."
        ),
        StructuredTool.from_function(
            func=get_hottest_cities,
            name="HottestCities",
            description="Utile pour trouver les villes les plus chaudes, classement températures élevées, canicule. Input: question sur chaleur."
        ),
        StructuredTool.from_function(
            func=get_coldest_cities,
            name="ColdestCities",
            description="Utile pour trouver les villes les plus froides, classement températures basses, gel, froid. Input: question sur froid."
        ),
        StructuredTool.from_function(
            func=get_global_stats,
            name="GlobalStats",
            description="Utile pour statistiques globales : moyennes mondiales, résumé général, nombre d'alertes. Input: question sur les stats."
        ),
        StructuredTool.from_function(
            func=get_most_humid_cities,
            name="HumidestCities",
            description="Utile pour trouver les villes les plus humides, classement par humidité, ville la plus humide, taux d humidité élevé. Input: question sur humidité."
        ),
        StructuredTool.from_function(
            func=get_most_precipitation,
            name="MostPrecipitation",
            description="Utile pour trouver les villes avec le plus de pluie, précipitations, villes pluvieuses. Input: question sur précipitations ou pluie."
        ),
        StructuredTool.from_function(
            func=get_windiest_cities,
            name="WindiestCities",
            description="Utile pour trouver les villes avec les vents les plus forts, classement par vitesse du vent, ville la plus venteuse. Input: question sur le vent."
        ),
        StructuredTool.from_function(
            func=get_weather_history,
            name="WeatherHistory",
            description="Utile pour l'historique météo, évolution, tendances des dernières heures. Input: question sur l'historique."
        ),
    ]

    # ─── AGENT ReAct (LangGraph) ──────────────────────────────────
    agent_executor = create_react_agent(model=llm, tools=tools)
    return agent_executor