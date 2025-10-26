#!/bin/bash

echo "Setting up SEC Filings Validation Pipeline..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Initialize Great Expectations
python run.py --init

# Run tests
pytest tests/ -v

# Create sample data
python -c "
from src.data_loader import SECDataLoader
loader = SECDataLoader()
df = loader.create_sample_data()
df.to_parquet('data/raw/sample_data.parquet')
print('✓ Sample data created')
"

echo "✓ Setup complete!"
echo "Run 'python run.py --sample' to test with sample data"
