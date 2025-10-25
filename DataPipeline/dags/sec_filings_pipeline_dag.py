"""
Airflow DAG for SEC Filings ETL Pipeline
Daily pipeline to download, extract, and convert SEC filings to parquet format

File location: dags/sec_filings_pipeline_dag.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

# Import custom operators from plugins folder
# Airflow automatically adds plugins/ to path
from plugins.sec_pipeline.operators import (
    check_companies_csv,
    download_sec_filings,
    extract_items_from_filings,
    convert_to_parquet,
    merge_parquet_files,
    send_success_notification,
    send_failure_notification,
    cleanup_temp_files
)

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),
}

# DAG definition
dag = DAG(
    'sec_filings_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for SEC filings: Download → Extract → Convert → Merge',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sec', 'filings', 'etl', 'daily'],
)

# Task 1: Check if companies CSV exists and is valid
check_csv_task = PythonOperator(
    task_id='check_companies_csv',
    python_callable=check_companies_csv,
    op_kwargs={
        'csv_path': Variable.get('companies_csv_path', 'config/companies.csv'),
    },
    dag=dag,
)

# Task 2: Download SEC filings
download_task = PythonOperator(
    task_id='download_sec_filings',
    python_callable=download_sec_filings,
    op_kwargs={
        'config_path': 'config.json',
    },
    dag=dag,
)

# Task 3: Extract items from downloaded filings
extract_task = PythonOperator(
    task_id='extract_items',
    python_callable=extract_items_from_filings,
    op_kwargs={
        'config_path': 'config.json',
    },
    dag=dag,
)

# Task 4: Convert JSON to Parquet
convert_task = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_to_parquet,
    op_kwargs={
        'config_path': 'config.json',
    },
    dag=dag,
)

# Task 5: Merge parquet files by filing type
merge_task = PythonOperator(
    task_id='merge_parquet_files',
    python_callable=merge_parquet_files,
    op_kwargs={
        'config_path': 'config.json',
    },
    dag=dag,
)

# Task 6: Cleanup temporary files (optional)
cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    op_kwargs={
        'config_path': 'config.json',
        'keep_json': True,  # Keep JSON files, only clean temp files
    },
    dag=dag,
)

# Task 7: Success notification
success_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    op_kwargs={
        'execution_date': '{{ ds }}',
    },
    dag=dag,
)

# Task 8: Failure notification (only runs on failure)
failure_task = PythonOperator(
    task_id='send_failure_notification',
    python_callable=send_failure_notification,
    trigger_rule='one_failed',
    op_kwargs={
        'execution_date': '{{ ds }}',
    },
    dag=dag,
)

# Define task dependencies
check_csv_task >> download_task >> extract_task >> convert_task >> merge_task >> cleanup_task >> success_task
[check_csv_task, download_task, extract_task, convert_task, merge_task] >> failure_task