"""
Data loading utilities for SEC filings
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import logging
from datetime import datetime
import pyarrow.parquet as pq

from config import RAW_DATA_PATH, VALIDATION_CONFIG

logger = logging.getLogger(__name__)

class SECDataLoader:
    """Handle data loading for SEC filings"""
    
    def __init__(self, data_path: Optional[str] = None):
        if data_path:
            self.data_path = Path(data_path) if isinstance(data_path, str) else data_path
        else:
            self.data_path = RAW_DATA_PATH
        self.df = None
        self.metadata = {}
        
    def load_data(self, sample_size: Optional[int] = None) -> pd.DataFrame:
        """Load SEC filings data"""
        logger.info(f"Loading data from {self.data_path}")
        
        try:
            # Ensure path is a Path object
            if not isinstance(self.data_path, Path):
                self.data_path = Path(self.data_path)
            
            if self.data_path.suffix == '.parquet':
                # Load the entire file first
                self.df = pd.read_parquet(self.data_path)
                
                # If sample size requested and it's less than total rows
                if sample_size and sample_size < len(self.df):
                    self.df = self.df.sample(n=sample_size, random_state=42)
                    
            elif self.data_path.suffix == '.csv':
                if sample_size:
                    self.df = pd.read_csv(self.data_path, nrows=sample_size)
                else:
                    self.df = pd.read_csv(self.data_path)
            else:
                raise ValueError(f"Unsupported file type: {self.data_path.suffix}")
                
            self._collect_metadata()
            logger.info(f"Loaded {len(self.df):,} records with {len(self.df.columns)} columns")
            return self.df
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            # Don't fall back - let it fail so we can see the real error
            raise
    
    def _collect_metadata(self):
        """Collect metadata about loaded data"""
        self.metadata = {
            'file_path': str(self.data_path),
            'load_timestamp': datetime.now().isoformat(),
            'num_rows': len(self.df),
            'num_columns': len(self.df.columns),
            'columns': list(self.df.columns),
            'dtypes': self.df.dtypes.to_dict(),
            'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024 / 1024
        }
    
    def validate_schema(self) -> Tuple[bool, Dict[str, Any]]:
        """Basic schema validation"""
        from config import SEC_SCHEMA_CONFIG
        
        expected = set(SEC_SCHEMA_CONFIG['expected_columns'])
        actual = set(self.df.columns)
        
        validation_result = {
            'is_valid': True,
            'missing_columns': list(expected - actual),
            'extra_columns': list(actual - expected),
            'column_count': len(actual)
        }
        
        if validation_result['missing_columns']:
            validation_result['is_valid'] = False
            logger.warning(f"Missing columns: {validation_result['missing_columns']}")
            
        return validation_result['is_valid'], validation_result
    

def load_sec_data(path: Optional[str] = None, sample: bool = False) -> pd.DataFrame:
    """Convenience function to load SEC data"""
    loader = SECDataLoader(path)
    return loader.load_data()

if __name__ == "__main__":
    # Test loading
    df = load_sec_data(sample=True)
    print(f"Loaded sample data: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(df.head())