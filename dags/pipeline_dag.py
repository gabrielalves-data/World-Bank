from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.extract import run_extract
from scripts.s3_upload import upload_raw_data
from utils.config_loader import load_config

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(minutes=30)
}

with DAG(
    dag_id="worldbank_pipeline",
    description="Daily World Bank GDP data pipeline: extract → S3 → Spark processing",
    schedule_interval="@daily",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=["wordlbank", "gdp", "etl"]
) as dag:
    # Extract data from API
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=run_extract,
        doc_md="""
        ### Extract Data
        Fetches World Bank GDP indicator data from the public REST API using
        pagination and saves the raw JSON response to `logs/raw_YYYY-MM-DD.json`.
        """
    )

    # Upload raw data to S3
    def _upload_to_s3() -> None:
        """Wrapper so upload_raw_data can be called without arguments."""
        config = load_config()
        upload_raw_data(config)

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_to_s3,
        doc_md="""
        ### Upload to S3
        Uploads the locally saved raw JSON file to S3 using Hive-style date
        partitioning: `s3://<bucket>/raw/world_bank/year=YYYY/month=MM/day=DD/data.json`.
        Requires valid AWS credentials (env vars, ~/.aws/credentials, or IAM role).
        """
    )

    # PySpark processing: clean data + analytics layer
    spark_processing = BashOperator(
        task_id="spark_preprocessing",
        bash_command="spark-submit /opt/airflow/scripts/spark_job.py",
        doc_md="""
        ### Spark Processing
        Runs the PySpark job via `spark-submit`.  The job:
        1. Reads raw JSON from S3 (wildcard across all date partitions)
        2. Cleans and casts columns → writes Parquet partitioned by year
        3. Aggregates average GDP per country → writes analytics Parquet
        """
    )

    extract_data >> upload_to_s3 >> spark_processing