"""
Weather Data Ingestion from OpenWeatherMap API
"""

import os
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Optional
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class WeatherDataIngestion:
    """Ingest weather data from OpenWeatherMap API to Snowflake"""
    
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('OPENWEATHERMAP_API_KEY')
        self.base_url = "https://api.openweathermap.org/data/2.5"
        
        # Snowflake connection parameters
        self.sf_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': 'BRONZE'
        }
        
        # Cities to monitor
        self.cities = self.get_city_list()
    
    def get_city_list(self) -> List[Dict]:
        """Get list of cities from Snowflake or default list"""
        try:
            conn = snowflake.connector.connect(**self.sf_params)
            cursor = conn.cursor()
            
            query = """
            SELECT CITY_NAME, COUNTRY_CODE, LATITUDE, LONGITUDE 
            FROM UTILS.CITY_COORDINATES
            """
            cursor.execute(query)
            cities = cursor.fetchall()
            
            if cities:
                return [
                    {
                        'name': row[0],
                        'country': row[1],
                        'lat': float(row[2]),
                        'lon': float(row[3])
                    }
                    for row in cities
                ]
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"Could not fetch cities from Snowflake: {e}")
        
        # Default cities if table doesn't exist
        return [
            {"name": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
            {"name": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
            {"name": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
            {"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522},
            {"name": "Sydney", "country": "AU", "lat": -33.8688, "lon": 151.2093},
        ]
    
    def fetch_current_weather(self, city: Dict) -> Optional[Dict]:
        """Fetch current weather for a city"""
        try:
            url = f"{self.base_url}/weather"
            params = {
                'lat': city['lat'],
                'lon': city['lon'],
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Extract relevant fields
            record = {
                'CITY_ID': data.get('id'),
                'CITY_NAME': city['name'],
                'COUNTRY_CODE': city['country'],
                'LATITUDE': data['coord']['lat'],
                'LONGITUDE': data['coord']['lon'],
                'TIMESTAMP': datetime.fromtimestamp(data['dt']),
                'INGESTION_TIMESTAMP': datetime.now(),
                'WEATHER_MAIN': data['weather'][0]['main'],
                'WEATHER_DESCRIPTION': data['weather'][0]['description'],
                'TEMPERATURE': data['main']['temp'],
                'FEELS_LIKE': data['main']['feels_like'],
                'TEMP_MIN': data['main']['temp_min'],
                'TEMP_MAX': data['main']['temp_max'],
                'PRESSURE': data['main']['pressure'],
                'HUMIDITY': data['main']['humidity'],
                'WIND_SPEED': data['wind'].get('speed', 0),
                'WIND_DEG': data['wind'].get('deg', 0),
                'CLOUD_COVERAGE': data['clouds'].get('all', 0),
                'VISIBILITY': data.get('visibility', 10000),
                'SUNRISE': datetime.fromtimestamp(data['sys']['sunrise']),
                'SUNSET': datetime.fromtimestamp(data['sys']['sunset']),
                'DATA_SOURCE': 'OPENWEATHERMAP_CURRENT'
            }
            
            logger.debug(f"Fetched weather for {city['name']}")
            return record
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {city['name']}: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Data parsing failed for {city['name']}: {e}")
            return None
    
    def create_bronze_table(self, conn):
        """Create bronze table if it doesn't exist"""
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS CURRENT_WEATHER_RAW (
            CITY_ID NUMBER,
            CITY_NAME VARCHAR(100),
            COUNTRY_CODE VARCHAR(10),
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            TIMESTAMP TIMESTAMP_NTZ,
            INGESTION_TIMESTAMP TIMESTAMP_NTZ,
            WEATHER_MAIN VARCHAR(50),
            WEATHER_DESCRIPTION VARCHAR(100),
            TEMPERATURE FLOAT,
            FEELS_LIKE FLOAT,
            TEMP_MIN FLOAT,
            TEMP_MAX FLOAT,
            PRESSURE NUMBER,
            HUMIDITY NUMBER,
            WIND_SPEED FLOAT,
            WIND_DEG NUMBER,
            CLOUD_COVERAGE NUMBER,
            VISIBILITY NUMBER,
            SUNRISE TIMESTAMP_NTZ,
            SUNSET TIMESTAMP_NTZ,
            DATA_SOURCE VARCHAR(50),
            INGESTION_DATE DATE DEFAULT CURRENT_DATE()
        )
        """
        
        cursor.execute(create_table_sql)
        cursor.close()
        logger.info("Bronze table created/verified")
    
    def ingest_weather_data(self) -> Dict:
        """Main ingestion method"""
        logger.info(f"Starting ingestion for {len(self.cities)} cities")
        
        all_records = []
        successful = 0
        failed = 0
        
        # Fetch weather for each city
        for idx, city in enumerate(self.cities, 1):
            logger.info(f"Processing {idx}/{len(self.cities)}: {city['name']}")
            
            record = self.fetch_current_weather(city)
            if record:
                all_records.append(record)
                successful += 1
            else:
                failed += 1
            
            # Rate limiting
            time.sleep(0.5)
        
        if not all_records:
            logger.error("No data was fetched")
            return {'status': 'FAILED', 'error': 'No data fetched'}
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        # Add ingestion date
        df['INGESTION_DATE'] = pd.Timestamp.now().date()
        
        # Connect to Snowflake and write data
        try:
            conn = snowflake.connector.connect(**self.sf_params)
            
            # Create table if needed
            self.create_bronze_table(conn)
            
            # Write DataFrame to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name='CURRENT_WEATHER_RAW',
                schema='BRONZE',
                quote_identifiers=False
            )
            
            conn.close()
            
            if success:
                logger.info(f"Successfully wrote {nrows} rows to Snowflake")
                result = {
                    'status': 'SUCCESS',
                    'records_ingested': len(all_records),
                    'successful_cities': successful,
                    'failed_cities': failed,
                    'cities': [city['name'] for city in self.cities[:5]]  # First 5 cities
                }
                return result
            else:
                logger.error("Failed to write data to Snowflake")
                return {'status': 'FAILED', 'error': 'Snowflake write failed'}
                
        except Exception as e:
            logger.error(f"Snowflake operation failed: {e}")
            return {'status': 'FAILED', 'error': str(e)}
    
    def test_api_connection(self) -> bool:
        """Test API connection"""
        try:
            test_city = {"name": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278}
            record = self.fetch_current_weather(test_city)
            return record is not None
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

if __name__ == "__main__":
    # Test the ingestion
    logging.basicConfig(level=logging.INFO)
    ingestor = WeatherDataIngestion()
    
    # Test API connection
    if ingestor.test_api_connection():
        print("✅ API connection successful")
        
        # Run ingestion
        result = ingestor.ingest_weather_data()
        print(f"Ingestion result: {result}")
    else:
        print("❌ API connection failed")
