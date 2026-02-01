from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

API_URL = "http://api:8000/truncate_ratings"


def call_truncate_ratings(**context):
    response = requests.post(API_URL)
    response.raise_for_status()
    return response.json()


with DAG(
    dag_id="truncate_ratings_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "ratings"],
):

    truncate_task = PythonOperator(
        task_id="truncate_ratings_table",
        python_callable=call_truncate_ratings,
    )
