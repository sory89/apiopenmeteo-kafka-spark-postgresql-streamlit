import streamlit as st
from langchain_helper import get_few_shot_db_chain

st.set_page_config(page_title="🌤️ Météo AI Agent", page_icon="🌤️", layout="wide")
st.title("🌤️ 🌤️ Météo Pipeline: Database Q&A")

st.markdown("Posez vos questions sur les données météo en temps réel")

question = st.text_input("Question: ", placeholder="Ex: Quelle est la température à Conakry?")

if question:
    with st.spinner("Analyse en cours..."):
        chain = get_few_shot_db_chain()
        response = chain(question)

    st.header("Réponse")

    st.write(response)
