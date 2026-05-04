"""
olist_pipeline_dag.py — Airflow DAG for the Olist E-Commerce Analytics pipeline.

Workflow (6 tasks):
    1. download_from_kaggle  — Download Olist CSVs from Kaggle API to local staging
    2. upload_raw_to_gcs     — Push raw CSV files to GCS data lake (raw zone)
    3. spark_transform       — PySpark: join all Olist CSVs → denormalized Parquet
    4. load_to_bigquery      — Load Parquet from GCS into BigQuery (partitioned + clustered)
    5. dbt_run               — Run dbt models: staging → dimensions → facts → aggregations
    6. dbt_test              — Run dbt data quality tests
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_DIR = "/home/airflow/gcs/dags/ecommerce-analytics"

ENV_EXPORT = (
    "export GCP_PROJECT_ID='{{ var.value.get(\"GCP_PROJECT_ID\", \"\") }}' && "
    "export GCP_REGION='{{ var.value.get(\"GCP_REGION\", \"us-central1\") }}' && "
    "export GCS_BUCKET='{{ var.value.get(\"GCS_BUCKET\", \"\") }}' && "
    "export BQ_RAW_DATASET='{{ var.value.get(\"BQ_RAW_DATASET\", \"olist_raw\") }}' && "
    "export BQ_PROD_DATASET='{{ var.value.get(\"BQ_PROD_DATASET\", \"olist_prod\") }}' && "
    "export KAGGLE_API_TOKEN='{{ var.value.get(\"KAGGLE_API_TOKEN\", \"\") }}' && "
    "export GOOGLE_APPLICATION_CREDENTIALS='{{ var.value.get(\"GOOGLE_APPLICATION_CREDENTIALS\", \"\") }}' && "
    ""
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_ecommerce_analytics_pipeline",
    default_args=default_args,
    description="Batch pipeline: Kaggle Olist → GCS → Spark → BigQuery → dbt",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "ecommerce", "batch", "warehouse"],
) as dag:

    download_from_kaggle = BashOperator(
        task_id="download_from_kaggle",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR} && "
            "python scripts/download_data.py"
        ),
    )

    upload_raw_to_gcs = BashOperator(
        task_id="upload_raw_to_gcs",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR} && "
            "python scripts/upload_to_gcs.py"
        ),
    )

    spark_transform = DataprocCreateBatchOperator(
        task_id="spark_transform",
        project_id="{{ var.value.get('GCP_PROJECT_ID', '') }}",
        region="{{ var.value.get('GCP_REGION', 'us-central1') }}",
        batch_id="olist-spark-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": (
                    "{{ var.value.get('COMPOSER_REPO_ROOT_GCS', '') }}"
                    "/spark/transform_events.py"
                ),
                "args": [
                    "--gcs-bucket",
                    "{{ var.value.get('GCS_BUCKET', '') }}",
                ],
            },
            "runtime_config": {"version": "2.2"},
            "environment_config": {
                "execution_config": {
                    "service_account": "{{ var.value.get('PIPELINE_SERVICE_ACCOUNT', '') }}",
                },
            },
        },
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR} && "
            "python scripts/load_to_bigquery.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR}/dbt && "
            "dbt deps --project-dir . --profiles-dir . && "
            "dbt run --project-dir . --profiles-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR}/dbt && "
            "dbt test --project-dir . --profiles-dir ."
        ),
    )

    (
        download_from_kaggle
        >> upload_raw_to_gcs
        >> spark_transform
        >> load_to_bigquery
        >> dbt_run
        >> dbt_test
    )
