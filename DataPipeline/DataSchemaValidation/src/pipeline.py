"""
Main pipeline for SEC filings validation
"""
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import json
from datetime import datetime
import pandas as pd

from data_loader import SECDataLoader
from ge_validator import SECDataSchemaGeneratorGE
from ge_setup import initialize_great_expectations
from config import OUTPUT_DIR, LOG_DIR
from email_alerter import EmailAlerter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SECValidationPipeline:
    """Main validation pipeline"""
    
    def __init__(self, data_path: Optional[str] = None):
        self.data_path = data_path
        self.loader = SECDataLoader(data_path)
        self.validator = SECDataSchemaGeneratorGE(data_path)
        self.context = initialize_great_expectations()
        self.results = {}
        self.alerter = EmailAlerter()
        
    def run(self, sample_size: Optional[int] = None) -> Dict[str, Any]:
        """Run complete validation pipeline"""
        
        logger.info("="*60)
        logger.info("Starting SEC Filings Validation Pipeline")
        logger.info("="*60)
        
        try:
            # Step 1: Load data
            logger.info("Step 1: Loading data...")
            df = self.loader.load_data(sample_size)
            self.results['data_loaded'] = {
                'rows': len(df),
                'columns': len(df.columns),
                'timestamp': datetime.now().isoformat()
            }
            
            # Step 2: Validate schema
            logger.info("Step 2: Validating schema...")
            is_valid, schema_validation = self.loader.validate_schema()
            self.results['schema_validation'] = schema_validation

            # Stop if schema is invalid
            if not is_valid:
                logger.error("="*60)
                logger.error("SCHEMA VALIDATION FAILED!")
                logger.error("="*60)
                if schema_validation['missing_columns']:
                    logger.error(f"Missing columns: {schema_validation['missing_columns']}")
                if schema_validation['extra_columns']:
                    logger.error(f"Extra columns: {schema_validation['extra_columns']}")
                
                # Create failure summary
                self.results['status'] = 'SCHEMA_FAILED'
                self.results['error'] = 'Schema validation failed - dataset does not match expected schema'
                
                # Generate failure report
                self._generate_summary()
                self._send_alert(schema_failure=True)
                
                logger.error("Pipeline stopped due to schema validation failure")
                raise ValueError(f"Schema validation failed. Missing columns: {schema_validation['missing_columns']}")
            
            logger.info("✓ Schema validation passed")
            
            # Step 3: Generate expectations
            logger.info("Step 3: Generating Great Expectations suite...")
            self.validator.df = df
            expectations = self.validator.generate_schema_expectations()
            self.results['expectations_generated'] = True
            
            # Step 4: Compute statistics
            logger.info("Step 4: Computing statistics...")
            stats = self.validator.generate_statistics()
            self.results['statistics'] = stats
            
            # Step 5: Validate quality
            logger.info("Step 5: Validating data quality...")
            quality_report = self.validator.validate_data_quality()
            self.results['quality_report'] = quality_report
            
            # Step 6: Export results
            logger.info("Step 6: Exporting results...")
            output_paths = self.validator.export_results(str(OUTPUT_DIR))
            self.results['output_paths'] = output_paths
            
            # Step 7: Generate summary
            self._generate_summary()
            
            logger.info("✓ Pipeline completed successfully")

            #Email
            if self.alerter and self.alerter.configured:
                self.alerter.send_validation_alert(self.results)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            self.results['error'] = str(e)
            raise
        
        return self.results
    
    def _generate_summary(self):
        """Generate pipeline summary"""
        
        # Check if schema validation failed
        if 'status' in self.results and self.results['status'] == 'SCHEMA_FAILED':
            summary = {
                'pipeline_run': datetime.now().isoformat(),
                'data_shape': f"{self.results['data_loaded']['rows']:,} × {self.results['data_loaded']['columns']}",
                'schema_valid': self.results['schema_validation']['is_valid'],
                'missing_columns': self.results['schema_validation'].get('missing_columns', []),
                'extra_columns': self.results['schema_validation'].get('extra_columns', []),
                'status': 'SCHEMA_FAILED',
                'error': self.results.get('error', 'Schema validation failed')
            }
        else:
            # Normal summary
            summary = {
                'pipeline_run': datetime.now().isoformat(),
                'data_shape': f"{self.results['data_loaded']['rows']:,} × {self.results['data_loaded']['columns']}",
                'schema_valid': self.results['schema_validation']['is_valid'],
                'quality_score': self.results['quality_report']['quality_score'],
                'status': 'PASSED' if self.results['quality_report']['quality_score'] >= 80 else 'FAILED'
            }
        
        # Print summary
        print("\n" + "="*60)
        print("PIPELINE SUMMARY")
        print("="*60)
        for key, value in summary.items():
            print(f"{key:20}: {value}")
        print("="*60)
        
        # Save summary
        summary_path = OUTPUT_DIR / f"pipeline_summary_{datetime.now():%Y%m%d_%H%M%S}.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.results['summary'] = summary

    def _send_alert(self, schema_failure: bool = False, pipeline_error: bool = False):
        """Send email alert based on pipeline results"""
        
        if not self.alerter or not self.alerter.configured:
            logger.info("Email alerts not configured")
            return
        
        try:
            # Determine alert type
            if schema_failure:
                alert_type = "SCHEMA FAILURE"
                logger.info(f"Sending {alert_type} alert...")
            elif pipeline_error:
                alert_type = "PIPELINE ERROR"
                logger.info(f"Sending {alert_type} alert...")
            else:
                quality_score = self.results.get('quality_report', {}).get('quality_score', 0)
                alert_type = "VALIDATION PASSED" if quality_score >= 80 else "VALIDATION FAILED"
                logger.info(f"Sending {alert_type} alert (Score: {quality_score:.1f}%)...")
            
            # Send email
            if self.alerter.send_validation_alert(self.results):
                logger.info(f"✅ {alert_type} alert sent successfully")
            else:
                logger.warning(f"❌ Failed to send {alert_type} alert")
                
        except Exception as e:
            logger.error(f"Error sending alert: {e}", exc_info=True)

def run_validation(data_path: Optional[str] = None, sample: bool = False):
    """Convenience function to run validation"""
    
    pipeline = SECValidationPipeline(data_path)
    
    if sample:
        # Run with sample data
        results = pipeline.run(sample_size=1000)
    else:
        # Run with full data
        results = pipeline.run()
    
    return results

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        data_path = sys.argv[1]
        results = run_validation(data_path)
    else:
        # Run with sample data for testing
        results = run_validation(sample=True)
