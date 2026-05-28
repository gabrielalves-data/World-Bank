import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.extract import run_extract
from scripts.s3_upload import upload_raw_data
from utils.config_loader import load_config


def _on_failure_callback(context: dict) -> None:
    """
    Called by Airflow when any task in this DAG fails.
    Logs structured failure details. Replace the logging calls below with
    your alerting integration (Slack, PagerDuty, email, etc.).
    """
    dag_id        = context["dag"].dag_id
    task_id       = context["task_instance"].task_id
    execution_date = context["logical_date"]
    log_url       = context["task_instance"].log_url
    exception     = context.get("exception")

    logging.error(
        "DAG FAILURE | dag=%s | task=%s | execution_date=%s | log=%s | error=%s",
        dag_id, task_id, execution_date, log_url, exception,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="worldbank_pipeline",
    description="Daily World Bank GDP data pipeline: extract → S3 → Spark processing",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["worldbank", "gdp", "etl"]
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
    # Path is configured via SPARK_JOB_PATH env var so it works across
    # different deployment layouts without touching DAG code.
    _spark_job_path = os.environ.get(
        "SPARK_JOB_PATH", "/opt/airflow/scripts/spark_job.py"
    )
    spark_processing = BashOperator(
        task_id="spark_preprocessing",
        bash_command=f"spark-submit {_spark_job_path}",
        doc_md="""
        ### Spark Processing
        Runs the PySpark job via `spark-submit`.  The job:
        1. Reads raw JSON from S3 (wildcard across all date partitions)
        2. Cleans and casts columns → writes Parquet partitioned by year
        3. Aggregates average GDP per country → writes analytics Parquet
        """
    )

    def _cleanup_old_logs(retention_days: int = 7) -> None:
        """Remove raw_*.json files from logs/ that are older than retention_days."""
        from pathlib import Path
        from datetime import datetime, timedelta

        logs_dir = Path(__file__).parent.parent / "logs"
        if not logs_dir.exists():
            return

        cutoff = datetime.now() - timedelta(days=retention_days)
        for log_file in logs_dir.glob("raw_*.json"):
            if datetime.fromtimestamp(log_file.stat().st_mtime) < cutoff:
                log_file.unlink()
                logging.info("Deleted old log file: %s", log_file.name)

    cleanup_logs = PythonOperator(
        task_id="cleanup_old_logs",
        python_callable=_cleanup_old_logs,
        doc_md="""
        ### Cleanup Old Logs
        Removes `raw_*.json` files from `logs/` that are older than 7 days
        to prevent unbounded disk growth on the Airflow worker.
        """
    )

    extract_data >> upload_to_s3 >> spark_processing >> cleanup_logs