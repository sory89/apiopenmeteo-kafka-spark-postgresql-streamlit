import streamlit as st
from weather_agent import build_agent
from few_shots import few_shots

st.set_page_config(page_title="🌤️ Météo AI Agent", page_icon="🌤️", layout="wide")
st.title("🌤️ Météo AI Agent")
st.markdown("**Agent IA** capable de choisir le bon outil selon votre question")

# ─── SIDEBAR ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🛠️ Outils disponibles")
    st.markdown("""
| Outil | Description |
|-------|-------------|
| 🗄️ WeatherDatabase | Question sur une ville |
| 🚨 WeatherAlerts | Alertes actives |
| 🔥 HottestCities | Villes les plus chaudes |
| 🧊 ColdestCities | Villes les plus froides |
| 📊 GlobalStats | Statistiques mondiales |
| 📈 WeatherHistory | Historique 6h |
    """)
    st.divider()
    st.markdown("**📋 Questions disponibles :**")
    for ex in few_shots:
        if st.button(ex['Question'], use_container_width=True, key=ex['Question']):
            st.session_state.pending_question = ex['Question']
    st.divider()
    if st.button("🗑️ Effacer la conversation", use_container_width=True):
        st.session_state.chat_history = []
        if "agent" in st.session_state:
            del st.session_state.agent
        st.rerun()

# ─── SESSION STATE ────────────────────────────────────────────────────
if "agent" not in st.session_state:
    with st.spinner("Initialisation de l'agent..."):
        st.session_state.agent = build_agent()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "pending_question" not in st.session_state:
    st.session_state.pending_question = ""

# ─── HISTORIQUE ───────────────────────────────────────────────────────
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# ─── HELPER: extraire texte de la réponse ────────────────────────────
def extract_answer(result) -> str:
    """Extrait le texte de la réponse de l'agent quelle que soit la structure."""
    try:
        messages = result.get("messages", [])
        if not messages:
            return str(result)

        last = messages[-1]

        # Cas 1 : attribut .content string
        if hasattr(last, "content"):
            content = last.content
            # Cas 2 : content est une liste de blocs (ex: [{"type":"text","text":"..."}])
            if isinstance(content, list):
                texts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        texts.append(block.get("text", ""))
                    elif isinstance(block, str):
                        texts.append(block)
                return " ".join(texts).strip()
            return str(content)

        return str(last)
    except Exception as e:
        return f"Erreur extraction: {e}"

# ─── INPUT ───────────────────────────────────────────────────────────
question = st.chat_input("Posez votre question météo...")

if not question and st.session_state.pending_question:
    question = st.session_state.pending_question
    st.session_state.pending_question = ""

if question:
    with st.chat_message("user"):
        st.write(question)
    st.session_state.chat_history.append({"role": "user", "content": question})

    with st.chat_message("assistant"):
        with st.spinner("L'agent réfléchit..."):
            try:
                result = st.session_state.agent.invoke(
                    {"messages": [("user", question)]}
                )
                answer = extract_answer(result)
                st.write(answer)
                st.session_state.chat_history.append({"role": "assistant", "content": answer})
            except Exception as e:
                st.error(f"Erreur: {e}")