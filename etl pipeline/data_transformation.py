"""
Data Transformation Module
Cleans, validates, and merges datasets with comprehensive quality checks
"""

import os
import pandas as pd
import numpy as np
import logging
from typing import Dict
from datetime import datetime

# Ensure logs folder exists
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles data transformation and quality validation"""
    
    def __init__(self):
        self.quality_metrics = {}
    
    def validate_data_quality(
        self, 
        df: pd.DataFrame, 
        dataset_name: str
    ) -> pd.DataFrame:
        """
        Comprehensive data quality validation
        
        Args:
            df: Input DataFrame
            dataset_name: Name of dataset for logging
        
        Returns:
            Validated DataFrame with quality metrics
        """
        logger.info(f"Validating {dataset_name} data quality...")
        
        initial_rows = len(df)
        quality_report = {
            'dataset': dataset_name,
            'initial_rows': initial_rows,
            'timestamp': datetime.now().isoformat()
        }
        
        # 1. Check for empty dataset
        if df.empty:
            logger.error(f"{dataset_name} dataset is empty")
            quality_report['status'] = 'FAILED - Empty dataset'
            self.quality_metrics[dataset_name] = quality_report
            return df
        
        # 2. Schema validation
        logger.info("Running schema validation...")
        required_columns = ['date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            quality_report['missing_columns'] = missing_columns
        
        # 3. Null value detection
        logger.info("Checking for null values...")
        null_counts = df.isnull().sum()
        null_percentage = (null_counts / len(df) * 100).round(2)
        
        if null_counts.sum() > 0:
            logger.warning(f"Found null values:\n{null_percentage[null_percentage > 0]}")
            quality_report['null_counts'] = null_counts.to_dict()
        
        # 4. Remove rows with critical nulls
        critical_columns = ['date']
        df_cleaned = df.dropna(subset=critical_columns)
        rows_removed_nulls = len(df) - len(df_cleaned)
        
        if rows_removed_nulls > 0:
            logger.info(f"Removed {rows_removed_nulls} rows with null critical values")
            quality_report['rows_removed_nulls'] = rows_removed_nulls
        
        # 5. Duplicate detection (full row)
        logger.info("Checking for duplicates (full row)...")
        duplicates = df_cleaned.duplicated(keep='first').sum()
        
        if duplicates > 0:
            logger.warning(f"Found {duplicates} duplicate records")
            df_cleaned = df_cleaned.drop_duplicates(keep='first')
            quality_report['duplicates_removed'] = duplicates
        
        # 6. Date range validation
        if 'date' in df_cleaned.columns:
            date_min = df_cleaned['date'].min()
            date_max = df_cleaned['date'].max()
            logger.info(f"Date range: {date_min} to {date_max}")
            quality_report['date_range'] = {
                'min': str(date_min),
                'max': str(date_max)
            }
        
        # 7. Numeric range validation
        numeric_cols = df_cleaned.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col != 'date':
                # Check for unrealistic values
                q1 = df_cleaned[col].quantile(0.01)
                q99 = df_cleaned[col].quantile(0.99)
                outliers = ((df_cleaned[col] < q1) | (df_cleaned[col] > q99)).sum()
                
                if outliers > 0:
                    logger.info(f"{col}: {outliers} potential outliers detected")
        
        # Calculate final quality score
        final_rows = len(df_cleaned)
        quality_score = (final_rows / initial_rows * 100) if initial_rows > 0 else 0
        
        quality_report['final_rows'] = final_rows
        quality_report['rows_removed'] = initial_rows - final_rows
        quality_report['quality_score'] = round(quality_score, 2)
        quality_report['status'] = 'PASSED' if quality_score >= 90 else 'WARNING'
        
        self.quality_metrics[dataset_name] = quality_report
        
        logger.info(f"Quality validation complete: {quality_score:.2f}% data retained")
        logger.info(f"Status: {quality_report['status']}")
        
        return df_cleaned
    
    def add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add calculated and derived features
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with additional features
        """
        logger.info("Adding derived features...")
        
        if 'date' not in df.columns:
            logger.warning("Cannot add date features - 'date' column missing")
            return df
        
        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Time-based features
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek
        df['day_name'] = df['date'].dt.day_name()
        df['week_of_year'] = df['date'].dt.isocalendar().week
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        df['is_weekday'] = (~df['day_of_week'].isin([5, 6])).astype(int)
        
        # Quarter and season
        df['quarter'] = df['date'].dt.quarter
        df['season'] = df['month'].map({
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        })
        
        # Weather-based features (if weather columns exist)
        if 'temperature_mean' in df.columns:
            df['temp_category'] = pd.cut(
                df['temperature_mean'],
                bins=[-np.inf, 32, 50, 68, 85, np.inf],
                labels=['Very Cold', 'Cold', 'Mild', 'Warm', 'Hot']
            )
        
        if 'precipitation' in df.columns:
            df['rain_category'] = pd.cut(
                df['precipitation'],
                bins=[-np.inf, 0.1, 2, 5, np.inf],
                labels=['No Rain', 'Light Rain', 'Moderate Rain', 'Heavy Rain']
            )
            df['is_rainy'] = (df['precipitation'] > 0.1).astype(int)
        
        # Weather impact score
        if all(col in df.columns for col in ['temperature_mean', 'precipitation', 'windspeed']):
            temp_norm = (df['temperature_mean'] - df['temperature_mean'].mean()) / df['temperature_mean'].std()
            precip_norm = df['precipitation'] / df['precipitation'].max()
            wind_norm = df['windspeed'] / df['windspeed'].max()
            df['weather_impact_score'] = abs(temp_norm) * 0.4 + precip_norm * 0.4 + wind_norm * 0.2
            
            df['weather_condition'] = pd.cut(
                df['weather_impact_score'],
                bins=[-np.inf, 0.3, 0.6, np.inf],
                labels=['Good', 'Moderate', 'Poor']
            )
        
        logger.info(f"Added {len([c for c in df.columns if c not in ['date']])} derived features")
        return df
    
    def transform_and_merge(
        self,
        ridership_df: pd.DataFrame,
        weather_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform and merge ridership and weather data
        """
        logger.info("=" * 60)
        logger.info("STARTING DATA TRANSFORMATION AND MERGE")
        logger.info("=" * 60)
        
        # Validate both datasets
        ridership_clean = self.validate_data_quality(ridership_df, "ridership")
        weather_clean = self.validate_data_quality(weather_df, "weather")
        
        if ridership_clean.empty or weather_clean.empty:
            logger.error("Cannot merge - one or both datasets are empty")
            return pd.DataFrame()
        
        # Standardize date columns
        ridership_clean['date'] = pd.to_datetime(ridership_clean['date']).dt.date
        weather_clean['date'] = pd.to_datetime(weather_clean['date']).dt.date
        
        # Merge datasets
        merged_df = pd.merge(
            ridership_clean,
            weather_clean,
            on='date',
            how='inner',
            suffixes=('_ridership', '_weather')
        )
        
        if merged_df.empty:
            logger.error("Merge resulted in empty dataset - no matching dates")
            return merged_df
        
        merged_df['date'] = pd.to_datetime(merged_df['date'])
        
        # Add derived features
        merged_df = self.add_derived_features(merged_df)
        
        # Rolling averages for ridership
        if 'ridership' in merged_df.columns:
            merged_df['ridership_7day_avg'] = merged_df['ridership'].rolling(window=7, min_periods=1).mean()
            merged_df['ridership_30day_avg'] = merged_df['ridership'].rolling(window=30, min_periods=1).mean()
        
        # Final quality check
        final_nulls = merged_df.isnull().sum().sum()
        final_quality_score = (1 - final_nulls / merged_df.size) * 100
        
        logger.info("=" * 60)
        logger.info("TRANSFORMATION COMPLETE")
        logger.info(f"Final dataset: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")
        logger.info(f"Final quality score: {final_quality_score:.2f}%")
        logger.info("=" * 60)
        
        return merged_df
    
    def get_quality_report(self) -> Dict:
        """Get comprehensive quality report"""
        return self.quality_metrics


def main():
    """Main execution for testing"""
    from data_extraction import DataExtractor
    
    logger.info("Starting transformation test...")
    
    extractor = DataExtractor()
    ridership_df = extractor.fetch_ridership_data()
    weather_df = extractor.fetch_weather_data()
    
    transformer = DataTransformer()
    merged_df = transformer.transform_and_merge(ridership_df, weather_df)
    
    if not merged_df.empty:
        print("\n✅ Transformation Complete!")
        print("\nMerged Data Sample:")
        print(merged_df.head())
        print(f"\nShape: {merged_df.shape}")
        print(f"\nColumns: {list(merged_df.columns)}")
        
        print("\n📊 Quality Report:")
        for dataset, metrics in transformer.get_quality_report().items():
            print(f"\n{dataset.upper()}:")
            for key, value in metrics.items():
                print(f"  {key}: {value}")


if __name__ == "__main__":
    os.makedirs('logs', exist_ok=True)
    main()
