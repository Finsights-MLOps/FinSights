import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))



from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src_metrics import data_ingestion
from src_metrics import data_loading
from src_metrics import data_preprocessing
from utils import notifier
from utils.helpers import setup_logger
import os 
import json
import boto3
from urllib.parse import urlparse


logger = setup_logger()

def ingest_data_task(**context):
    bucket = os.environ["S3_BUCKET_NAME"]
    raw_data = data_ingestion.ingest_data(n=2)  # dict {ticker: facts}
    payload = json.dumps(raw_data, ensure_ascii=False).encode("utf-8")

    key = f"raw/{context['dag'].dag_id}/{context['task'].task_id}/{context['run_id'].replace(':','_')}.json"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="application/json")

    context["ti"].xcom_push(key="raw_s3_uri", value=f"s3://{bucket}/{key}")
    return None  # IMPORTANT


def preprocess_data_task(**context):
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='qualitative_extraction')
    if not raw_data:
        logger.warning("No raw data received from ingestion task")
        return None

    # Build big list via your pandas function
    processed = []
    for ticker, facts in raw_data.items():
        processed.extend(data_preprocessing.process_company_data(ticker, facts))

    # (A) safest: use default adapter
    def _json_default(o):
        try:
            import numpy as np
            if isinstance(o, np.integer): return int(o)
            if isinstance(o, np.floating): return float(o)
            if o is np.nan: return None
        except Exception:
            pass
        try:
            import pandas as pd
            if o is pd.NA: return None
        except Exception:
            pass
        return str(o)

    payload = json.dumps(processed, ensure_ascii=False, default=_json_default).encode("utf-8")

    bucket = os.environ["S3_BUCKET_NAME"]
    key = f"preprocessed/{context['dag'].dag_id}/{context['task'].task_id}/{context['run_id'].replace(':','_')}.json"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="application/json")

    ti.xcom_push(key="metrics_s3_uri", value=f"s3://{bucket}/{key}")
    return None  # IMPORTANT: don't return the big payload


def load_data_task(**context):
    ti = context["ti"]
    s3_uri = ti.xcom_pull(task_ids="preprocess_data", key="metrics_s3_uri")
    if not s3_uri:
        logger.warning("No S3 URI from preprocess step")
        return None

    u = urlparse(s3_uri)                 # s3://bucket/key -> Parse
    bucket, key = u.netloc, u.path.lstrip("/")

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(obj["Body"].read().decode("utf-8"))

    logger.info(f"Loaded {len(data):,} records from {s3_uri}")
    # (Optional) run validations here…

    return s3_uri   # tiny return; still set do_xcom_push=False on operator

default_args = {
    "owner": "Finsights",
    "depends_on_past": False,
    "email_on_failure": False,  # we'll handle manually
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}




with DAG(
    dag_id="sec_10k_pipeline",
    default_args=default_args,
    description="End-to-end SEC 10-K RAG + Metric Extraction Pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sec", "rag", "financial-analysis"],
    is_paused_upon_creation=True,
) as dag:

    data_ingest = PythonOperator(
        task_id="qualitative_extraction",
        python_callable=ingest_data_task,
        do_xcom_push=False, 
    )

    data_preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data_task,
        do_xcom_push=False, 
    )

    data_load = PythonOperator(
        task_id="load_to_s3",
        python_callable=load_data_task,
        do_xcom_push=False, 
    )

    notify_failure_task = PythonOperator(
        task_id="notify_failure",
        python_callable=lambda: notifier.send_notification(
            "SEC 10-K Pipeline Failure ",
            "One or more pipeline tasks failed. Please review Airflow logs."
        ),
        trigger_rule="one_failed",  # triggers only if a previous task fails
    )

    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=lambda: notifier.send_notification(
            "SEC 10-K Pipeline Success ",
            "All pipeline tasks completed successfully."
        ),
        trigger_rule="all_success",  # triggers only if all previous tasks succeed
    )

    # Define task dependencies
    data_ingest >> data_preprocess >> data_load
    
    # Both notification tasks depend on data_load, but trigger based on different rules
    data_load >> [notify_failure_task, notify_success_task]
