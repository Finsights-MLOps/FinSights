from pathlib import Path
import os

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
# Directories relative to project root
DATASET_DIR = AIRFLOW_HOME / "datasets"
LOGGING_DIR = AIRFLOW_HOME / "logs"
# LOGGING_DIR.mkdir(parents=True, exist_ok=True)

os.makedirs(LOGGING_DIR, exist_ok=True)
os.makedirs(DATASET_DIR, exist_ok=True)
