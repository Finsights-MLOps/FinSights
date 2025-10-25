"""
Custom operators and callable functions for SEC Filings Airflow Pipeline

File location: plugins/sec_pipeline/operators.py

This module contains all the callable functions used by the Airflow DAG.
It imports business logic from the src/ directory.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List
import pandas as pd
import polars as pl
from datetime import datetime


project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)


from src.download_filings import main as download_main
from src.extract_and_convert import ExtractItems, convert_json_to_parquet
from __init__ import DATASET_DIR 


logger = logging.getLogger(__name__)


def check_companies_csv(csv_path: str, **context) -> Dict:
    """
    Check if companies CSV exists and contains valid data.
    
    Args:
        csv_path: Path to the companies CSV file
        
    Returns:
        dict: Validation results with company count
        
    Raises:
        FileNotFoundError: If CSV doesn't exist
        ValueError: If CSV is empty or invalid
    """
    logger.info(f"Checking companies CSV at: {csv_path}")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Companies CSV not found at {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        
        if df.empty:
            raise ValueError("Companies CSV is empty")
        
        # Check for required columns (CIK or Ticker)
        if 'CIK' not in df.columns and 'Ticker' not in df.columns:
            raise ValueError("CSV must contain either 'CIK' or 'Ticker' column")
        
        company_count = len(df)
        logger.info(f"Found {company_count} companies in CSV")
        
        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(key='company_count', value=company_count)
        
        return {
            'status': 'success',
            'company_count': company_count,
            'csv_path': csv_path
        }
        
    except Exception as e:
        logger.error(f"Error reading companies CSV: {e}")
        raise


def download_sec_filings(config_path: str, **context) -> Dict:
    """
    Download SEC filings using the existing download_filings.py module.
    
    Args:
        config_path: Path to config.json
        
    Returns:
        dict: Download statistics
    """
    logger.info("Starting SEC filings download...")
    
    try:
        # Load config to get download settings
        with open(config_path) as f:
            config = json.load(f)
        
        download_config = config.get('download_filings', {})
        
        logger.info(f"Downloading filings for years: {download_config.get('start_year')} - {download_config.get('end_year')}")
        logger.info(f"Filing types: {download_config.get('filing_types')}")
        
        # Run the main download function
        download_main()
        
        # Get metadata file to count downloads
        metadata_path = os.path.join(DATASET_DIR, download_config.get('filings_metadata_file', 'filings_metadata.csv'))
        
        if os.path.exists(metadata_path):
            df = pd.read_csv(metadata_path)
            download_count = len(df)
            logger.info(f"Total filings in metadata: {download_count}")
            
            context['task_instance'].xcom_push(key='download_count', value=download_count)
        else:
            download_count = 0
            logger.warning("Metadata file not found after download")
        
        return {
            'status': 'success',
            'download_count': download_count,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error during download: {e}")
        raise


def extract_items_from_filings(config_path: str, **context) -> Dict:
    """
    Extract items from downloaded filings and save as JSON.
    
    Args:
        config_path: Path to config.json
        
    Returns:
        dict: Extraction statistics
    """
    logger.info("Starting item extraction from filings...")
    
    try:
        with open(config_path) as f:
            config = json.load(f)
        
        extract_config = config.get('extract_items', {})
        
        # Get paths
        raw_folder = os.path.join(DATASET_DIR, extract_config.get('raw_filings_folder'))
        extracted_folder = os.path.join(DATASET_DIR, extract_config.get('extracted_filings_folder'))
        metadata_file = os.path.join(DATASET_DIR, extract_config.get('filings_metadata_file'))
        
        if not os.path.exists(metadata_file):
            raise FileNotFoundError(f"Filings metadata file not found: {metadata_file}")
        
        # Read metadata
        df = pd.read_csv(metadata_file, dtype=str)
        df = df.replace({pd.NA: None})
        
        # Filter by filing types
        if extract_config.get('filing_types'):
            df = df[df['Type'].isin(extract_config['filing_types'])]
        
        total_to_extract = len(df)
        logger.info(f"Extracting items from {total_to_extract} filings...")
        
        # Initialize extractor
        extraction = ExtractItems(
            remove_tables=extract_config.get('remove_tables', True),
            items_to_extract=extract_config.get('items_to_extract', []),
            include_signature=extract_config.get('include_signature', False),
            raw_files_folder=raw_folder,
            extracted_files_folder=extracted_folder,
            skip_extracted_filings=extract_config.get('skip_extracted_filings', True),
        )
        
        # Process filings
        extracted_count = 0
        for _, filing_metadata in df.iterrows():
            try:
                result = extraction.process_filing(filing_metadata)
                if result:
                    extracted_count += 1
            except Exception as e:
                logger.warning(f"Failed to extract {filing_metadata.get('filename')}: {e}")
        
        logger.info(f"Successfully extracted {extracted_count} / {total_to_extract} filings")
        
        context['task_instance'].xcom_push(key='extracted_count', value=extracted_count)
        
        return {
            'status': 'success',
            'extracted_count': extracted_count,
            'total_filings': total_to_extract,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        raise


def convert_to_parquet(config_path: str, **context) -> Dict:
    """
    Convert extracted JSON files to parquet format.
    
    Args:
        config_path: Path to config.json
        
    Returns:
        dict: Conversion statistics
    """
    logger.info("Starting JSON to Parquet conversion...")
    
    try:
        with open(config_path) as f:
            config = json.load(f)
        
        extract_config = config.get('extract_items', {})
        
        extracted_folder = os.path.join(DATASET_DIR, extract_config.get('extracted_filings_folder'))
        parquet_folder = os.path.join(DATASET_DIR, "PARQUET_FILES")
        
        os.makedirs(parquet_folder, exist_ok=True)
        
        # Find all JSON files
        json_files = []
        filing_types = []
        
        if os.path.exists(extracted_folder):
            for item in os.listdir(extracted_folder):
                item_path = os.path.join(extracted_folder, item)
                if os.path.isdir(item_path):
                    filing_types.append(item)
                    for f in os.listdir(item_path):
                        if f.endswith('.json'):
                            json_files.append(os.path.join(item_path, f))
        
        if not json_files:
            logger.warning(f"No JSON files found in {extracted_folder}")
            return {'status': 'success', 'converted_count': 0}
        
        logger.info(f"Found {len(json_files)} JSON files to convert")
        
        # Convert files
        converted_count = 0
        failed_count = 0
        
        for json_file in json_files:
            try:
                filing_type = Path(json_file).parent.name
                output_folder = os.path.join(parquet_folder, filing_type)
                
                if convert_json_to_parquet(
                    json_path=json_file,
                    output_folder=output_folder,
                    min_year=extract_config.get('min_year', 2018),
                    max_year=extract_config.get('max_year', 2025)
                ):
                    converted_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.warning(f"Error converting {json_file}: {e}")
                failed_count += 1
        
        logger.info(f"Converted {converted_count} files, {failed_count} failed")
        
        context['task_instance'].xcom_push(key='converted_count', value=converted_count)
        
        return {
            'status': 'success',
            'converted_count': converted_count,
            'failed_count': failed_count,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        raise


def merge_parquet_files(config_path: str, **context) -> Dict:
    """
    Merge individual parquet files into single files by filing type.
    
    Args:
        config_path: Path to config.json
        
    Returns:
        dict: Merge statistics
    """
    logger.info("Starting parquet file merge...")
    
    try:
        with open(config_path) as f:
            config = json.load(f)
        
        extract_config = config.get('extract_items', {})
        
        extracted_folder = os.path.join(DATASET_DIR, extract_config.get('extracted_filings_folder'))
        parquet_folder = os.path.join(DATASET_DIR, "PARQUET_FILES")
        
        # Find filing types
        filing_types = []
        if os.path.exists(parquet_folder):
            filing_types = [
                d for d in os.listdir(parquet_folder)
                if os.path.isdir(os.path.join(parquet_folder, d))
            ]
        
        if not filing_types:
            logger.warning("No filing type folders found to merge")
            return {'status': 'success', 'merged_count': 0}
        
        logger.info(f"Merging parquet files for {len(filing_types)} filing types")
        
        merge_count = 0
        merge_stats = {}
        
        for filing_type in filing_types:
            parquet_type_folder = os.path.join(parquet_folder, filing_type)
            
            # Get all parquet files
            parquet_files = [
                os.path.join(parquet_type_folder, f)
                for f in os.listdir(parquet_type_folder)
                if f.endswith('.parquet')
            ]
            
            if not parquet_files:
                continue
            
            logger.info(f"Merging {len(parquet_files)} files for {filing_type}...")
            
            try:
                # Read with Polars
                dfs = []
                for pf in parquet_files:
                    try:
                        df = pl.read_parquet(pf)
                        dfs.append(df)
                    except Exception as e:
                        logger.warning(f"Failed to read {pf}: {e}")
                
                if dfs:
                    # Merge
                    merged_df = pl.concat(dfs, how="vertical")
                    merged_df = merged_df.sort(["docID", "section_ID", "sentence_index"])
                    
                    # Save
                    output_folder = os.path.join(extracted_folder, filing_type)
                    os.makedirs(output_folder, exist_ok=True)
                    
                    merged_filename = f"{filing_type}_merged_sentences.parquet"
                    merged_filepath = os.path.join(output_folder, merged_filename)
                    
                    merged_df.write_parquet(merged_filepath, compression="snappy")
                    
                    # Also save CSV
                    csv_filepath = os.path.join(output_folder, f"{filing_type}_merged_sentences.csv")
                    merged_df.write_csv(csv_filepath)
                    
                    sentence_count = len(merged_df)
                    logger.info(f"✓ Merged {filing_type}: {sentence_count} sentences")
                    
                    merge_stats[filing_type] = {
                        'files_merged': len(parquet_files),
                        'total_sentences': sentence_count,
                        'output_path': merged_filepath
                    }
                    
                    merge_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to merge {filing_type}: {e}")
        
        logger.info(f"Merged {merge_count} filing types successfully")
        
        context['task_instance'].xcom_push(key='merge_count', value=merge_count)
        context['task_instance'].xcom_push(key='merge_stats', value=merge_stats)
        
        return {
            'status': 'success',
            'merge_count': merge_count,
            'merge_stats': merge_stats,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error during merge: {e}")
        raise


def cleanup_temp_files(config_path: str, keep_json: bool = True, **context) -> Dict:
    """
    Clean up temporary files after processing.
    
    Args:
        config_path: Path to config.json
        keep_json: Whether to keep JSON files (default: True)
        
    Returns:
        dict: Cleanup statistics
    """
    logger.info("Starting cleanup of temporary files...")
    
    try:
        with open(config_path) as f:
            config = json.load(f)
        
        cleanup_count = 0
        
        # Optional: Clean up individual parquet files if merged files exist
        parquet_folder = os.path.join(DATASET_DIR, "PARQUET_FILES")
        
        if os.path.exists(parquet_folder):
            for filing_type in os.listdir(parquet_folder):
                type_folder = os.path.join(parquet_folder, filing_type)
                if os.path.isdir(type_folder):
                    # Check if merged file exists
                    extracted_folder = os.path.join(DATASET_DIR, config['extract_items']['extracted_filings_folder'])
                    merged_file = os.path.join(extracted_folder, filing_type, f"{filing_type}_merged_sentences.parquet")
                    
                    if os.path.exists(merged_file):
                        # Only delete individual CSV files to save space
                        for f in os.listdir(type_folder):
                            if f.endswith('.csv'):
                                os.remove(os.path.join(type_folder, f))
                                cleanup_count += 1
        
        logger.info(f"Cleaned up {cleanup_count} temporary files")
        
        return {
            'status': 'success',
            'cleanup_count': cleanup_count,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")
        # Don't fail the pipeline on cleanup errors
        return {
            'status': 'partial',
            'cleanup_count': 0,
            'error': str(e)
        }


def send_success_notification(execution_date: str, **context) -> None:
    """
    Send notification on successful pipeline completion.
    
    Args:
        execution_date: DAG execution date
    """
    task_instance = context['task_instance']
    
    # Get statistics from previous tasks
    company_count = task_instance.xcom_pull(task_ids='check_companies_csv', key='company_count') or 0
    download_count = task_instance.xcom_pull(task_ids='download_sec_filings', key='download_count') or 0
    extracted_count = task_instance.xcom_pull(task_ids='extract_items', key='extracted_count') or 0
    converted_count = task_instance.xcom_pull(task_ids='convert_to_parquet', key='converted_count') or 0
    merge_count = task_instance.xcom_pull(task_ids='merge_parquet_files', key='merge_count') or 0
    
    message = f"""
    SEC Filings ETL Pipeline - SUCCESS
    
    Execution Date: {execution_date}
    
    Pipeline Statistics:
    - Companies tracked: {company_count}
    - Filings downloaded: {download_count}
    - Items extracted: {extracted_count}
    - Files converted: {converted_count}
    - Filing types merged: {merge_count}
    
    Status: ✓ All tasks completed successfully
    """
    
    logger.info(message)
    
    # TODO: Add email/Slack notification here
    # Example: send_slack_message(message) or send_email(message)
    
    print(message)


def send_failure_notification(execution_date: str, **context) -> None:
    """
    Send notification on pipeline failure.
    
    Args:
        execution_date: DAG execution date
    """
    task_instance = context['task_instance']
    
    message = f"""
    SEC Filings ETL Pipeline - FAILURE
    
    Execution Date: {execution_date}
    
    Status: ✗ Pipeline encountered errors
    
    Please check Airflow logs for details.
    """
    
    logger.error(message)
    
    # TODO: Add email/Slack notification here
    # Example: send_slack_alert(message) or send_email(message)
    
    print(message)