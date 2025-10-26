
#!/usr/bin/env python

"""

Main execution script for SEC filings validation

"""

import click

import logging

from pathlib import Path

import sys

sys.path.append(str(Path(__file__).parent / 'src'))

from pipeline import run_validation

from ge_setup import initialize_great_expectations

from data_loader import load_sec_data

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

@click.command()

@click.option('--data-path', '-d', help='Path to data file')

@click.option('--sample', '-s', is_flag=True, help='Use sample data')

@click.option('--sample-size', '-n', type=int, help='Sample size')

@click.option('--init', is_flag=True, help='Initialize Great Expectations')

def main(data_path, sample, sample_size, init):

    """SEC Filings Validation Pipeline"""

    

    print("\n" + "="*60)

    print("SEC FILINGS VALIDATION PIPELINE")

    print("="*60 + "\n")

    

    if init:

        print("Initializing Great Expectations...")

        context = initialize_great_expectations()

        print("✓ Great Expectations initialized")

        return

    

    if sample:

        print("Running with sample data...")

        results = run_validation(sample=True)

    elif sample_size:

        print(f"Running with {sample_size} samples...")

        from pipeline import SECValidationPipeline

        pipeline = SECValidationPipeline(data_path)

        results = pipeline.run(sample_size=sample_size)

    else:

        print(f"Running with data from: {data_path or 'default path'}")

        results = run_validation(data_path)

    

    print("\n✓ Validation complete!")

    print(f"Quality Score: {results['quality_report']['quality_score']:.1f}%")

if __name__ == "__main__":

    main()

