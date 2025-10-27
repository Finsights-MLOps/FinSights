"""
Configuration management for SEC Filings validation pipeline
"""
import os
from pathlib import Path
from typing import Dict, List, Any
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs"
GE_DIR = BASE_DIR / "great_expectations"
LOG_DIR = BASE_DIR / "logs"

# S3 Configuration  
S3_CONFIG = {
    'enabled': True,  # Set to True to use S3
    'bucket_name': 'sentence-data-ingestion',
    's3_key': 'DATA_MERGE_ASSETS/HISTORICAL_DATA/finrag_sec_fact_historical.parquet',
}

# Data paths
RAW_DATA_PATH = f"s3://{S3_CONFIG['bucket_name']}/{S3_CONFIG['s3_key']}"
PROCESSED_DATA_PATH = DATA_DIR / "processed"
REFERENCE_DATA_PATH = DATA_DIR / "reference"

# Great Expectations paths
GE_CONFIG_PATH = GE_DIR / "great_expectations.yml"
EXPECTATIONS_PATH = GE_DIR / "expectations"
CHECKPOINTS_PATH = GE_DIR / "checkpoints"
DATA_DOCS_PATH = GE_DIR / "data_docs"

# Validation settings
VALIDATION_CONFIG = {
    'missing_value_threshold': 0.1,  # 10% max missing
    'quality_score_threshold': 0.8,  # 80% min quality
    'anomaly_detection_threshold': 0.05,  # 5% max anomalies
    'batch_size': 10000,  # For chunked processing
}

# SEC Filing specific configurations - UPDATED FOR 24 COLUMNS
SEC_SCHEMA_CONFIG = {
    'expected_columns': [
        'cik', 'cik_int', 'name', 'tickers', 'docID', 'sentenceID',
        'section_ID', 'section_name', 'form', 'sic', 'sentence',
        'filingDate', 'report_year', 'reportDate', 'temporal_bin',
        'likely_kpi', 'has_numbers', 'has_comparison',
        'sample_created_at', 'last_modified_date', 'sample_version',
        'source_file_path', 'load_method', 'row_hash'
    ],
    'numeric_columns': {
        'cik_int': {'min': 0, 'max': 9999999999},
        'report_year': {'min': 2000, 'max': 2030},
        'sic': {'min': 0, 'max': 9999},
        'section_ID': {'min': 0, 'max': 20}
    },
    'categorical_columns': {
        'form': ['10-K'],
        'section_name': ['ITEM_1', 'ITEM_1A', 'ITEM_2', 'ITEM_3', 'ITEM_7', 'ITEM_8', 'ITEM_9', 'ITEM_10', 'ITEM_11', 'ITEM_12', 'ITEM_13', 'ITEM_14', 'ITEM_15'],
        'temporal_bin': ['bin_2006_2009', 'bin_2010_2015', 'bin_2016_2020', 'bin_2021_2025'],
        'load_method': ['stratified_sampling', 'random_sampling', 'full_load', 'extract_and_convert']
    },
    'boolean_columns': ['likely_kpi', 'has_numbers', 'has_comparison'],
    'text_columns': {
        'sentence': {'min_length': 10, 'max_length': 10000},
        'section_name': {'min_length': 3, 'max_length': 100}
    },
    'date_columns': ['filingDate', 'reportDate', 'sample_created_at', 'last_modified_date'],
    'identifier_columns': ['cik', 'cik_int', 'docID', 'sentenceID', 'row_hash'],
    'unique_combinations': [['docID', 'sentenceID']],  # These should be unique together
    'string_columns': ['cik', 'name', 'docID', 'sentenceID', 'section_name', 
                      'form', 'sentence', 'temporal_bin', 
                      'sample_version', 'source_file_path', 'load_method', 'row_hash'],
    'list_columns': ['tickers']  # Column that contains lists
}

# Monitoring configuration
MONITORING_CONFIG = {
    'enable_metrics': True,
    'metrics_port': 8000,
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'alert_email': os.getenv('ALERT_EMAIL', 'data-team@company.com'),
}

# Airflow configuration
AIRFLOW_CONFIG = {
    'dag_id': 'sec_filings_validation',
    'schedule_interval': '@daily',
    'max_active_runs': 1,
    'catchup': False,
}

def load_config(config_file: str = None) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    if config_file and Path(config_file).exists():
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    return {}

def save_config(config: Dict[str, Any], config_file: str):
    """Save configuration to YAML file"""
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)