from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


def run_extract_movies(**_context):
    from scripts.extract_api import main as extract_movies

    extract_movies()


def run_transform_movies(**_context):
    from scripts.transform_spark import main as transform_movies

    transform_movies()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="movie_etl_pipeline",
    description="Extract movies from TMDB and transform them with Spark.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["movies", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_tmdb_movies",
        python_callable=run_extract_movies,
    )

    transform_task = PythonOperator(
        task_id="transform_movies",
        python_callable=run_transform_movies,
    )

    finish = EmptyOperator(task_id="finish")

    start >> extract_task >> transform_task >> finish
