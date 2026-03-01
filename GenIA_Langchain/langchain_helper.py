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

from few_shots import few_shots

PROMPT_SUFFIX = """Only use the following tables:
{table_info}

Question: {input}"""

_postgres_prompt = """You are a PostgreSQL expert. Given an input question, first create a syntactically correct PostgreSQL query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per PostgreSQL. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Pay attention to use NOW() instead of date('now') for current date/time queries.
Pay attention to use ROUND(value::numeric, 2) for rounding in PostgreSQL.

Use the following format:

Question: Question here
SQLQuery: Query to run with no pre-amble
SQLResult: Result of the SQLQuery
Answer: Final answer here

No pre-amble.
"""

def get_few_shot_db_chain():
    db = SQLDatabase.from_uri(
        "postgresql+psycopg2://weather_user:weather_pass@localhost:5432/weather",
        sample_rows_in_table_info=3
    )
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=os.environ["GOOGLE_API_KEY"],
        temperature=0.1
    )
    embeddings = HuggingFaceEmbeddings(model_name='sentence-transformers/all-MiniLM-L6-v2')
    to_vectorize = [" ".join(str(v) for v in example.values()) for example in few_shots]
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

    def run_chain(question: str) -> str:
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
            result = db.run(sql_query)
        except Exception as e:
            return f"Erreur SQL: {e}"
        answer_prompt = PromptTemplate.from_template("""
Question: {question}
SQL: {sql}
Résultat: {result}
Réponds en français de façon claire et concise.
""")
        return (answer_prompt | llm | StrOutputParser()).invoke({
            "question": question,
            "sql": sql_query,
            "result": result
        })

    return run_chain
