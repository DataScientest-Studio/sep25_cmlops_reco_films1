import os
import requests
import streamlit as st
import yaml

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Reco Films", layout="centered")
st.title("Recommandation de films")

tab1, tab2 = st.tabs(["Prédire une évaluation", "Recommander des films"])

with tab1:
    st.subheader("Prédire une évaluation")
    user_id = st.number_input("User ID", min_value=1, step=1)
    movie_id = st.number_input("Movie ID", min_value=1, step=1)
    if st.button("Prédire"):
        resp = requests.post(f"{API_URL}/predict", json={"user_id": int(user_id), "movie_id": int(movie_id)})
        if resp.ok:
            data = resp.json()
            st.success(f"Évaluation prédite: {data['predicted_rating']:.2f}")
            st.caption(f"Temps chargement: {data['load_time']:.3f}s | Temps prédiction: {data['prediction_time']:.3f}s")
        else:
            st.error(resp.text)

with tab2:
    st.subheader("Recommandations")
    user_id_reco = st.number_input("User ID (reco)", min_value=1, step=1, key="user_id_reco")
    n = st.slider("Nombre de recommandations", 1, 20, 5)
    if st.button("Recommander"):
        resp = requests.post(f"{API_URL}/recommend", json={"user_id": int(user_id_reco), "n_recommendations": int(n)})
        if resp.ok:
            data = resp.json()
            st.table([
                {
                    "Movie ID": reco[0],
                    "Titre": reco[1],
                    "Genres": reco[2],
                    "Score": f"{float(reco[3]):.2f}"
                }
                for reco in data["recommendations"]
            ])
        else:
            st.error(resp.text)