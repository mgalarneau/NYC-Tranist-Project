"""
Data Loading Module
Loads processed data to various destinations with error handling
"""

import pandas as pd
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
from sqlalchemy import create_engine, text
import json
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/loading.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Handles data loading to various destinations"""
    
    def __init__(self, connection_string: str = None, s3_bucket: str = None):
        """
        Initialize DataLoader
        
        Args:
            connection_string: PostgreSQL connection string
            s3_bucket: AWS S3 bucket name for uploads
        """
        self.connection_string = connection_string or os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:postgres@localhost:5432/nyc_transit_db'
        )
        self.s3_bucket = s3_bucket or os.getenv('S3_BUCKET', 'nyc-transit-weather')
        self.load_metrics = {}
    
    # -------------------------
    # CSV / JSON Saving
    # -------------------------
    def save_to_csv(
        self, 
        df: pd.DataFrame, 
        filename: str, 
        output_dir: str = "data/processed"
    ) -> str:
        """Save DataFrame to CSV"""
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(filename)
        filename = f"{name}_{timestamp}{ext}"
        filepath = os.path.join(output_dir, filename)
        try:
            df.to_csv(filepath, index=False)
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            self.load_metrics['csv'] = {
                'filepath': filepath,
                'records': len(df),
                'size_mb': round(file_size_mb, 2),
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"✅ Saved {len(df)} records to {filepath} ({file_size_mb:.2f} MB)")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save CSV: {str(e)}")
            raise

    def save_to_json(
        self, 
        df: pd.DataFrame, 
        filename: str, 
        output_dir: str = "data/processed"
    ) -> str:
        """Save DataFrame to JSON"""
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, _ = os.path.splitext(filename)
        filename = f"{name}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        try:
            df.to_json(filepath, orient='records', date_format='iso', indent=2)
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            logger.info(f"✅ Saved {len(df)} records to {filepath} ({file_size_mb:.2f} MB)")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save JSON: {str(e)}")
            raise

    # -------------------------
    # AWS S3 Upload
    # -------------------------
    def upload_to_s3(self, local_file: str, s3_key: str):
        """Upload local file to S3"""
        if not self.s3_bucket:
            logger.warning("S3 bucket not configured. Skipping upload.")
            return
        try:
            s3 = boto3.client('s3')
            s3.upload_file(local_file, self.s3_bucket, s3_key)
            logger.info(f"✅ Uploaded {local_file} to s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise

    # -------------------------
    # PostgreSQL / RDS Loading
    # -------------------------
    def load_to_postgres(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'replace'
    ) -> bool:
        """Load DataFrame to PostgreSQL database"""
        logger.info(f"Loading data to PostgreSQL table: {table_name}")
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            start_time = datetime.now()
            df.to_sql(
                table_name,
                engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            load_time = (datetime.now() - start_time).total_seconds()
            self.load_metrics['postgresql'] = {
                'table': table_name,
                'records': len(df),
                'load_time_seconds': round(load_time, 2),
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"✅ Loaded {len(df)} records to {table_name} in {load_time:.2f}s")
            return True
        except Exception as e:
            logger.error(f"Failed to load to PostgreSQL: {str(e)}")
            logger.info("Falling back to CSV save...")
            self.save_to_csv(df, f"{table_name}_backup.csv")
            return False

    # -------------------------
    # Summary Stats
    # -------------------------
    def create_summary_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create summary statistics DataFrame"""
        logger.info("Creating summary statistics...")
        summary = {'metric': [], 'value': []}
        summary['metric'].append('Total Records'); summary['value'].append(len(df))
        summary['metric'].append('Date Range Start'); summary['value'].append(str(df['date'].min()) if 'date' in df.columns else 'N/A')
        summary['metric'].append('Date Range End'); summary['value'].append(str(df['date'].max()) if 'date' in df.columns else 'N/A')
        summary['metric'].append('Number of Columns'); summary['value'].append(len(df.columns))
        if 'ridership' in df.columns:
            summary['metric'].append('Avg Daily Ridership'); summary['value'].append(f"{df['ridership'].mean():,.0f}")
            summary['metric'].append('Max Ridership'); summary['value'].append(f"{df['ridership'].max():,.0f}")
            summary['metric'].append('Min Ridership'); summary['value'].append(f"{df['ridership'].min():,.0f}")
        if 'temperature_mean' in df.columns:
            summary['metric'].append('Avg Temperature (°F)'); summary['value'].append(f"{df['temperature_mean'].mean():.1f}")
        if 'precipitation' in df.columns:
            summary['metric'].append('Avg Precipitation (mm)'); summary['value'].append(f"{df['precipitation'].mean():.2f}")
        return pd.DataFrame(summary)

    # -------------------------
    # Metrics
    # -------------------------
    def save_load_metrics(self, output_dir: str = "data/processed"):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        filepath = os.path.join(output_dir, 'load_metrics.json')
        with open(filepath, 'w') as f:
            json.dump(self.load_metrics, f, indent=2)
        logger.info(f"Saved load metrics to {filepath}")

    def get_load_metrics(self) -> dict:
        """Get loading metrics"""
        return self.load_metrics