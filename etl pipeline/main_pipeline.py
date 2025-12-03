"""
Main ETL Pipeline
Orchestrates the complete data pipeline with monitoring, error handling, and S3 uploads
"""

import logging
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import os
import json

# Import pipeline modules
from data_extraction import DataExtractor
from data_transformation import DataTransformer
from data_loading import DataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""

    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.pipeline_metrics = {
            'start_time': None,
            'end_time': None,
            'status': 'NOT_STARTED',
            'records_processed': 0,
            'errors': []
        }

    def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        save_to_db: bool = True
    ) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
    
        # Initialize metrics timers to 0 to avoid NameError
        extraction_time = 0
        transformation_time = 0
        loading_time = 0
    
        self.pipeline_metrics['start_time'] = datetime.now().isoformat()
        self.pipeline_metrics['status'] = 'RUNNING'

        logger.info("=" * 70)
        logger.info(" STARTING NYC TRANSIT WEATHER ANALYTICS PIPELINE")
        logger.info("=" * 70)

        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        logger.info(f" Date Range: {start_date} to {end_date}")
        logger.info(f" Save to Database: {save_to_db}")

        try:
            # -------------------------------
            # PHASE 1: EXTRACTION
            # -------------------------------
            extraction_start = datetime.now()
            logger.info("Extracting ridership data...")
            ridership_df = self.extractor.fetch_ridership_data(start_date=start_date, end_date=end_date)
            if ridership_df.empty:
                raise ValueError("No ridership data extracted")
            logger.info(f" Ridership: {len(ridership_df)} records")

            logger.info("Extracting weather data...")
            # Fixed: use keyword arguments to avoid misinterpreting dates as lat/lon
            weather_df = self.extractor.fetch_weather_data(
                start_date=start_date,
                end_date=end_date
            )
            if weather_df.empty:
                raise ValueError("No weather data extracted")
            logger.info(f" Weather: {len(weather_df)} records")


            # -------------------------------
            # PHASE 2: TRANSFORMATION
            # -------------------------------
            transformation_start = datetime.now()
            merged_df = self.transformer.transform_and_merge(ridership_df, weather_df)
            if merged_df.empty:
                raise ValueError("Transformation resulted in empty dataset")
            transformation_time = (datetime.now() - transformation_start).total_seconds()
            self.pipeline_metrics['transformation'] = {
                'output_records': len(merged_df),
                'output_columns': len(merged_df.columns),
                'duration_seconds': round(transformation_time, 2),
                'quality_metrics': self.transformer.get_quality_report()
            }

            # -------------------------------
            # PHASE 3: LOADING
            # -------------------------------
            loading_start = datetime.now()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save CSV locally
            csv_path = self.loader.save_to_csv(merged_df, f"transit_weather_{timestamp}.csv")
            summary_df = self.loader.create_summary_stats(merged_df)
            summary_path = self.loader.save_to_csv(summary_df, f"summary_stats_{timestamp}.csv")

            # Upload CSVs to S3 if bucket configured
            try:
                self.loader.upload_to_s3(csv_path, f"transit_weather/{os.path.basename(csv_path)}")
                self.loader.upload_to_s3(summary_path, f"transit_weather/{os.path.basename(summary_path)}")
            except Exception as e:
                logger.warning(f"S3 upload failed: {str(e)}")

            # Save to PostgreSQL/RDS if requested
            db_success = False
            if save_to_db:
                db_success = self.loader.load_to_postgres(merged_df, "transit_weather_analytics")

            loading_time = (datetime.now() - loading_start).total_seconds()
            self.pipeline_metrics['loading'] = {
                'csv_path': csv_path,
                'summary_path': summary_path,
                'database_success': db_success,
                'duration_seconds': round(loading_time, 2),
                'load_metrics': self.loader.get_load_metrics()
            }

            # -------------------------------
            # PHASE 4: METRICS
            # -------------------------------
            total_time = extraction_time + transformation_time + loading_time
            null_count = merged_df.isnull().sum().sum()
            total_cells = merged_df.size
            quality_score = (1 - null_count / total_cells) * 100 if total_cells > 0 else 0
            success_rate = (len(merged_df) / max(len(ridership_df), len(weather_df))) * 100

            self.pipeline_metrics.update({
                'end_time': datetime.now().isoformat(),
                'status': 'SUCCESS',
                'records_processed': len(merged_df),
                'total_duration_seconds': round(total_time, 2),
                'quality_score': round(quality_score, 2),
                'success_rate': round(success_rate, 2)
            })

            self._save_pipeline_metrics()
            logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
            return {'status': 'SUCCESS', 'data': merged_df, 'metrics': self.pipeline_metrics}

        except Exception as e:
            logger.error(f"PIPELINE FAILED: {str(e)}")
            logger.exception("Exception details:")
            self.pipeline_metrics.update({
                'end_time': datetime.now().isoformat(),
                'status': 'FAILED',
                'errors': [{'error': str(e), 'timestamp': datetime.now().isoformat()}]
            })
            self._save_pipeline_metrics()
            return {'status': 'FAILED', 'error': str(e), 'metrics': self.pipeline_metrics}

    def _save_pipeline_metrics(self):
        """Save pipeline metrics to JSON"""
        os.makedirs('data/processed', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/processed/pipeline_metrics_{timestamp}.json"
        with open(filepath, 'w') as f:
            json.dump(self.pipeline_metrics, f, indent=2, default=str)
        logger.info(f"Pipeline metrics saved to {filepath}")


def parse_arguments():
    parser = argparse.ArgumentParser(description='NYC Transit Weather ETL Pipeline')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--no-db', action='store_true', help='Skip database loading')
    return parser.parse_args()


def main():
    os.makedirs('logs', exist_ok=True)
    args = parse_arguments()
    pipeline = ETLPipeline()
    result = pipeline.run(start_date=args.start_date, end_date=args.end_date, save_to_db=not args.no_db)

    if result['status'] == 'SUCCESS':
        print("\nPIPELINE EXECUTION SUMMARY")
        print(f"Status: {result['status']}")
        print(f"Records Processed: {result['metrics']['records_processed']:,}")
        print(f"Total Time: {result['metrics']['total_duration_seconds']} seconds")
        print(f"Quality Score: {result['metrics']['quality_score']}%")
        if 'data' in result and not result['data'].empty:
            print("\nSample Output (first 5 rows):")
            print(result['data'].head().to_string())
        sys.exit(0)
    else:
        print("\nPIPELINE FAILED")
        print(f"Error: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
