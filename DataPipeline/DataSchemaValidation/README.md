# SEC Filings Data Validation Pipeline

MLOps pipeline for validating SEC 10-K filing data using Great Expectations with automated email alerts.

## Overview
This pipeline validates SEC filing datasets by checking schema compliance, data quality metrics, and statistical properties. It automatically sends email notifications for validation results.

## Features
- **Schema Validation**: Ensures all 20 required columns are present
- **Data Quality Checks**: Validates missing values, duplicates, temporal consistency
- **Statistical Profiling**: Generates comprehensive data statistics
- **Automated Alerts**: Email notifications for pass/fail results
- **Great Expectations Integration**: Industry-standard data validation framework

## Installation

**Requirements**: Python 3.11

```bash
# Clone repository
git clone <repository-url>
cd DataSchemaValidation

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize Great Expectations
python run.py --init
```

## Configuration

1. **Update data path** in `config.py`:
```python
RAW_DATA_PATH = Path("path/to/your/data.parquet")
```

## Usage

### Run validation on your data:
```bash
python run.py --data-path path/to/data.parquet
```

## Expected Data Schema

The pipeline expects 20 columns in the following format:
- `cik`, `name`, `report_year`, `docID`, `sentenceID`
- `section_name`, `section_item`, `section_ID`, `form`, `sentence_index`  
- `sentence`, `SIC`, `filingDate`, `reportDate`, `temporal_bin`
- `sample_created_at`, `last_modified_date`, `sample_version`
- `source_file_path`, `load_method`

## Outputs

Results are saved in the `outputs/` directory:
- `pipeline_summary_*.json` - Validation summary
- `statistics_*.json` - Data statistics
- `quality_report_*.json` - Quality metrics
- `validation_report_*.html` - HTML report

## Quality Metrics

- **PASSED**: Quality score ≥ 80%
- **FAILED**: Quality score < 80%
- **SCHEMA_FAILED**: Missing or incorrect columns

## Project Structure
```
sec-filings-mlops-ge/
├── src/
│   ├── pipeline.py         # Main validation pipeline
│   ├── ge_validator.py     # Great Expectations validator
│   ├── data_loader.py      # Data loading utilities
│   ├── email_alerter.py    # Email notification system
│   └── config.py           # Configuration settings
├── data/
│   └── raw/               # Input data files
├── outputs/               # Validation results
├── great_expectations/    # GE configuration
├── run.py                # Main execution script
└── requirements.txt      # Python dependencies
```

##Note: For email alerts:(add this as a .env file)

# Email Configuration for SEC Validation Pipeline

SMTP Settings

SMTP_SERVER=smtp.gmail.com

SMTP_PORT=587

# Sender Credentials

SENDER_EMAIL= #sender gmail

SENDER_PASSWORD=#app password (setup with the gamil account using the link below)

#https://myaccount.google.com/apppasswords
  # Use app password for Gmail

ALERT_EMAIL=#recipient email

LOG_LEVEL=INFO
