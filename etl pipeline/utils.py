"""
Utility Functions
Helper functions used across the pipeline
"""

import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


def setup_logging(log_dir: str = 'logs', log_level: str = 'INFO'):
    """
    Set up logging configuration
    
    Args:
        log_dir: Directory for log files
        log_level: Logging level
    """
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'{log_dir}/pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )


def validate_date_format(date_str: str) -> bool:
    """
    Validate date string format
    
    Args:
        date_str: Date string to validate
    
    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def get_date_range(days_back: int = 365) -> tuple:
    """
    Get date range for data extraction
    
    Args:
        days_back: Number of days to go back from today
    
    Returns:
        Tuple of (start_date, end_date) as strings
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    return (
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )


def calculate_data_size(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate size metrics for DataFrame
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        Dictionary with size metrics
    """
    memory_usage = df.memory_usage(deep=True).sum()
    
    return {
        'rows': len(df),
        'columns': len(df.columns),
        'memory_mb': round(memory_usage / (1024 * 1024), 2),
        'cells': df.size
    }


def print_dataframe_summary(df: pd.DataFrame, name: str = "DataFrame"):
    """
    Print a formatted summary of DataFrame
    
    Args:
        df: DataFrame to summarize
        name: Name for the summary
    """
    print(f"\n{'=' * 60}")
    print(f"{name} Summary")
    print('=' * 60)
    print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(f"\nColumns: {', '.join(df.columns)}")
    print(f"\nData Types:\n{df.dtypes.value_counts()}")
    print(f"\nNull Values:\n{df.isnull().sum()[df.isnull().sum() > 0]}")
    print('=' * 60)


if __name__ == "__main__":
    # Test utilities
    print("Testing utility functions...")
    
    print(f"\nDate validation: {validate_date_format('2024-01-01')}")
    print(f"Date range: {get_date_range(30)}")