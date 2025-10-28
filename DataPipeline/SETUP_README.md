# FinSights - DataPipeline 

### 1. Clone & Setup Environment
```bash
git clone https://github.com/Finsights-MLOps/MLOps_Final.git
cd MLOps_Final/DataPipeline
conda env create -f environment.yml
conda activate finsight-venv
```

### 2. Configure Credentials
```bash
cp .env.template .env
# Edit .env with AWS credentials. Please reach out to any team member of Group 4 for the access keys
```

### 3. Start Airflow
```bash
docker compose up airflow-init
docker compose up
```

### 4️. Access UI
Open: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 5. Trigger Pipeline
Click ▶️ next to `sec_filings_etl_pipeline`

For Validation & Statistics: [To be integrated into Docker-Airflow.]
```bash
cd data_auto_stats/src
python run_validation.py
python run_statistics.py
```

---

## Common Commands
```bash
# View logs
docker compose logs -f

# Stop Airflow
docker compose down

# Restart services
docker compose restart

# Run tests
pytest tests/ -v
```
