"""
Project initialization and constants
This file defines the root directories for datasets and logs.
"""

import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

# Define dataset and logging directories at project root level
DATASET_DIR = os.path.join(CURRENT_DIR, 'datasets')
LOGGING_DIR = os.path.join(CURRENT_DIR, 'logs')

# Create directories if they don't exist
if not os.path.exists(DATASET_DIR):
    os.makedirs(DATASET_DIR, exist_ok=True)
    print(f"Created DATASET_DIR: {DATASET_DIR}")

if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR, exist_ok=True)
    print(f"Created LOGGING_DIR: {LOGGING_DIR}")

# Export all constants
__all__ = ['CURRENT_DIR', 'DATASET_DIR', 'LOGGING_DIR']