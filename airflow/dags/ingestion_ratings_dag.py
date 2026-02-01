from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

# URL du service FastAPI exposant l’endpoint d’ingestion.
API_URL = "http://api:8000/load_ratings"

# chemin des fichiers
DATA_PATH = "/opt/airflow/data/raw/ml-20m"
PROCESSED_PATH = "/opt/airflow/data/raw/ml-20m/processed"

def detect_new_files(**context):
    # Tous les fichiers ratings-*.csv présents dans le dossier
    all_files = sorted([
        f for f in os.listdir(DATA_PATH)
        if f.startswith("ratings") and f.endswith(".csv")
    ])

    # Détecter les nouveaux fichiers (ceux qui restent dans DATA_PATH)
    print(f"Nouveaux fichiers détectés : {all_files}")

    return all_files


def call_load_ratings(**context):
    new_files = context["ti"].xcom_pull(task_ids="detect_new_files")

    # Si aucun nouveau fichier → on ne fait rien
    if not new_files:
        return []

    print(f"Chargement des fichiers : {new_files}")

    response = requests.post(API_URL, json={"fileNames": new_files})
    response.raise_for_status()

    # On renvoie la liste des fichiers chargés avec succès
    return response.json()["success"]


def move_processed_files(**context):
    processed_files = context["ti"].xcom_pull(task_ids="load_ratings") or []

    if not processed_files:
        return []

    os.makedirs(PROCESSED_PATH, exist_ok=True)

    moved = []
    for file_name in processed_files:
        src = os.path.join(DATA_PATH, file_name)
        dst = os.path.join(PROCESSED_PATH, file_name)
        if os.path.exists(src):
            os.rename(src, dst)
            moved.append(file_name)

    print(f"Fichiers déplacés vers processed : {moved}")
    return moved



# Définition du DAG d’ingestion.
with DAG(
    dag_id="ingestion_ratings_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Exécution manuelle ou déclenchée par un autre DAG.
    catchup=False,
    tags=["etl", "ratings"],
):

    detect_task = PythonOperator( 
        task_id="detect_new_files", 
        python_callable=detect_new_files 
        )
    
    ingestion_task = PythonOperator(
        task_id="load_ratings",
        python_callable=call_load_ratings

        )

    move_processed_task = PythonOperator(
        task_id="move_processed_files",
        python_callable=move_processed_files
    )
    # Déclenchement automatique du DAG d’entraînement à la fin de l’ingestion. 
    trigger_training = TriggerDagRunOperator( 
        task_id="trigger_training", 
        trigger_dag_id="training_svd_dag" 
    ) 

    # Dépendance : l’entraînement démarre uniquement si l’ingestion est terminée avec succès. 
    detect_task >> ingestion_task >> move_processed_task >> trigger_training