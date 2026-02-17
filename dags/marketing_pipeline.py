from datetime import datetime, timedelta
import pathlib

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

PROJECT_DIR = "/opt/airflow/project"

def read_sql(rel_path: str) -> str:
    return pathlib.Path(f"{PROJECT_DIR}/{rel_path}").read_text()

default_args = {
    "owner": "shruthi",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="marketing_analytics_pipeline",
    default_args=default_args,
    description="Transform -> Load staging -> Load dims -> Load fact -> Create KPI views",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # manual trigger for your demo
    catchup=False,
    tags=["marketing", "etl", "postgres"],
) as dag:

    transform = BashOperator(
        task_id="transform_csv",
        bash_command=f"cd {PROJECT_DIR} && python src/transform_data.py",
    )

    load_staging = BashOperator(
        task_id="load_to_staging",
        bash_command=f"cd {PROJECT_DIR} && python src/load_to_staging.py",
    )

    load_dims = PostgresOperator(
        task_id="load_dimensions",
        postgres_conn_id="analytics_db",
        sql=read_sql("sql/02_load_dimensions.sql"),
    )

    load_fact = PostgresOperator(
        task_id="load_fact",
        postgres_conn_id="analytics_db",
        sql=read_sql("sql/03_load_fact.sql"),
    )

    refresh_kpis = PostgresOperator(
        task_id="refresh_kpi_views",
        postgres_conn_id="analytics_db",
        sql=read_sql("sql/04_kpi_views.sql"),
    )

    transform >> load_staging >> load_dims >> load_fact >> refresh_kpis
