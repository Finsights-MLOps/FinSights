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

# Data paths
RAW_DATA_PATH = Path("/Users/vishaknair/Downloads/sec-filings-mlops-ge/data/raw/10-K_merged_2020_2023_ALL.parquet")
# RAW_DATA_PATH = Path("/Users/vishaknair/Downloads/sec-filings-mlops-ge/data/raw/finrag_21companies_allbins_v2.0.parquet")
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

# SEC Filing specific configurations - UPDATED FOR 20 COLUMNS
SEC_SCHEMA_CONFIG = {
    'expected_columns': [
        'cik', 'name', 'report_year', 'docID', 'sentenceID',
        'section_name', 'section_item', 'section_ID', 'form', 'sentence_index',
        'sentence', 'SIC', 'filingDate', 'reportDate', 'temporal_bin',
        'sample_created_at', 'last_modified_date', 'sample_version',
        'source_file_path', 'load_method'
    ],
    'numeric_columns': {
        'report_year': {'min': 2000, 'max': 2030},
        'SIC': {'min': 0, 'max': 9999},
        'sentence_index': {'min': 0, 'max': 10000},
        'section_ID': {'min': 0, 'max': 20}
    },
    'categorical_columns': {
        'form': ['10-K'],
        'section_item': ['ITEM_1', 'ITEM_1A', 'ITEM_2', 'ITEM_3', 'ITEM_7', 'ITEM_8'],
        'temporal_bin': ['bin_2006_2009', 'bin_2010_2015', 'bin_2016_2020', 'bin_2021_2025'],
        'load_method': ['extract_and_convert', 'stratified_sampling', 'random_sampling', 'full_load']
    },
    'text_columns': {
        'sentence': {'min_length': 10, 'max_length': 10000},
        'section_name': {'min_length': 3, 'max_length': 100}
    },
    'date_columns': ['filingDate', 'reportDate', 'sample_created_at', 'last_modified_date'],
    'identifier_columns': ['cik', 'docID', 'sentenceID'],
    'unique_combinations': [['docID', 'sentenceID']],  # These should be unique together
    'string_columns': ['cik', 'name', 'docID', 'sentenceID', 'section_name', 
                      'section_item', 'form', 'sentence', 'temporal_bin', 
                      'sample_version', 'source_file_path', 'load_method']
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