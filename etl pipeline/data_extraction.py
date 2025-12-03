"""
Data Extraction - FINAL WORKING VERSION
Fetches data properly across multiple days
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from dotenv import load_dotenv

load_dotenv()
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extraction.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self):
        self.mta_api_url = "https://data.ny.gov/resource/kv7t-n8in.json"
        self.weather_api_url = "https://archive-api.open-meteo.com/v1/archive"
        self.max_retries = 3
        self.retry_delay = 2

    def fetch_with_retry(self, func, max_retries=None, delay=None):
        max_retries = max_retries or self.max_retries
        delay = delay or self.retry_delay
        
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    raise

    def fetch_ridership_data(self, start_date=None, end_date=None, max_records=600000):
        """
        Fetch by fetching MULTIPLE DAYS separately, then combining
        This avoids the API pagination bug
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = "2023-01-01"
        
        logger.info("=" * 70)
        logger.info(f"FETCHING RIDERSHIP: {start_date} to {end_date}")
        logger.info(f"Strategy: Fetch day-by-day to avoid API limits")
        logger.info("=" * 70)
        
        # Generate date range
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end_dt - start_dt).days + 1
        
        all_data = []
        records_per_day = max_records // days if days > 0 else 50000
        
        logger.info(f"Will fetch {days} days, ~{records_per_day} records per day")
        
        for day_offset in range(0, days, 7):  # Fetch week by week
            current_date = start_dt + timedelta(days=day_offset)
            week_end = min(current_date + timedelta(days=6), end_dt)
            
            date_str = current_date.strftime("%Y-%m-%d")
            week_end_str = week_end.strftime("%Y-%m-%d")
            
            def _fetch():
                params = {
                    "$limit": records_per_day * 7,
                    "$order": "transit_timestamp DESC",
                    "$where": f"transit_timestamp >= '{date_str}T00:00:00' AND transit_timestamp < '{week_end_str}T23:59:59'"
                }
                
                headers = {}
                api_token = os.getenv("SOCRATA_APP_TOKEN")
                if api_token:
                    headers["X-App-Token"] = api_token
                
                response = requests.get(self.mta_api_url, params=params, headers=headers, timeout=60)
                response.raise_for_status()
                return response.json()
            
            try:
                batch = self.fetch_with_retry(_fetch)
                if batch:
                    all_data.extend(batch)
                    logger.info(f"Week {date_str} to {week_end_str}: {len(batch)} records | Total: {len(all_data)}")
                
                if len(all_data) >= max_records:
                    logger.info(f"Reached target of {max_records}")
                    all_data = all_data[:max_records]
                    break
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Failed to fetch week {date_str}: {e}")
                continue
        
        if not all_data:
            logger.error("No data fetched!")
            return pd.DataFrame()
        
        logger.info(f"TOTAL FETCHED: {len(all_data)} records")
        
        df = pd.DataFrame(all_data)
        
        if 'transit_timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['transit_timestamp'], errors='coerce')
        
        for col in ['ridership', 'transfers']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna(subset=['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        logger.info(f"Final: {len(df)} records from {df['date'].min()} to {df['date'].max()}")
        
        return df

    def fetch_weather_data(self, lat=40.7128, lon=-74.0060, start_date=None, end_date=None):
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = "2023-01-01"

        def _fetch():
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date,
                "end_date": end_date,
                "daily": "temperature_2m_mean,precipitation_sum,windspeed_10m_max",
                "timezone": "America/New_York"
            }
            response = requests.get(self.weather_api_url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()

        data = self.fetch_with_retry(_fetch)
        daily = data.get("daily", {})
        
        df = pd.DataFrame({
            "date": pd.to_datetime(daily.get("time", [])),
            "temperature_mean": daily.get("temperature_2m_mean", []),
            "precipitation": daily.get("precipitation_sum", []),
            "windspeed": daily.get("windspeed_10m_max", [])
        })

        logger.info(f"Weather: {len(df)} records from {df['date'].min().date()} to {df['date'].max().date()}")
        
        return df

    def save_raw_data(self, df, filename):
        os.makedirs('data/raw', exist_ok=True)
        filepath = os.path.join('data/raw', filename)
        df.to_csv(filepath, index=False)
        logger.info(f"Saved to {filepath}")


if __name__ == "__main__":
    extractor = DataExtractor()
    
    ridership_df = extractor.fetch_ridership_data(
        start_date="2023-01-01",
        end_date="2024-12-31",
        max_records=600000
    )
    
    print(f"\nRidership: {len(ridership_df)} records")
    print(f"Date range: {ridership_df['date'].min()} to {ridership_df['date'].max()}")
    print(ridership_df.head())
    
    weather_df = extractor.fetch_weather_data(start_date="2023-01-01", end_date="2024-12-31")
    
    print(f"\nWeather: {len(weather_df)} records")
    print(weather_df.head())