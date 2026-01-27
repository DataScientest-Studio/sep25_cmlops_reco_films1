from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

API_URL = "http://api:8000/training"

def call_training(**context):
    payload = {"limit": None}  # ou un nombre si tu veux limiter
    response = requests.post(API_URL, json=payload)
    response.raise_for_status()
    return response.json()

with DAG(
    dag_id="training_svd_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["training", "svd"],
):

    training_task = PythonOperator(
        task_id="train_svd_model",
        python_callable=call_training,
    )
