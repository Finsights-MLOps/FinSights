from pathlib import Path
import os

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
LOGGING_DIR = AIRFLOW_HOME / "logs"
LOGGING_DIR.mkdir(parents=True, exist_ok=True)
