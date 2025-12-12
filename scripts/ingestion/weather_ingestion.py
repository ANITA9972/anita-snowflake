# ingestion_fixed.py
import os
import logging
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class WeatherDataIngestion:
    """Ingest weather data from OpenWeatherMap API to Bronze layer"""
    
    def __init__(self):
        load_dotenv()
        
        # Snowflake connection
        self.sf_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': 'BRONZE'
        }
        
        # OpenWeatherMap API
        self.api_key = os.getenv('OPENWEATHERMAP_API_KEY')
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        
        # Default cities if no database connection
        self.default_cities = [
            {'city': 'New York', 'country': 'US', 'lat': 40.7128, 'lon': -74.0060},
            {'city': 'London', 'country': 'GB', 'lat': 51.5074, 'lon': -0.1278},
            {'city': 'Tokyo', 'country': 'JP', 'lat': 35.6762, 'lon': 139.6503},
            {'city': 'Paris', 'country': 'FR', 'lat': 48.8566, 'lon': 2.3522},
            {'city': 'Sydney', 'country': 'AU', 'lat': -33.8688, 'lon': 151.2093}
        ]
    
    def get_cities_from_snowflake(self):
        """Try to get cities from Snowflake, fallback to defaults"""
        try:
            conn = snowflake.connector.connect(**self.sf_params)
            cursor = conn.cursor()
            
            # Check if CITIES table exists in UTILS schema
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'UTILS' 
                AND TABLE_NAME = 'CITIES'
            """)
            
            if cursor.fetchone():
                cursor.execute("SELECT city, country_code, latitude, longitude FROM UTILS.CITIES LIMIT 10")
                cities = cursor.fetchall()
                logger.info(f"Found {len(cities)} cities in Snowflake")
                return [{'city': c[0], 'country': c[1], 'lat': float(c[2]), 'lon': float(c[3])} for c in cities]
            else:
                logger.warning("UTILS.CITIES table not found, using default cities")
                return self.default_cities
                
        except Exception as e:
            logger.warning(f"Could not fetch cities from Snowflake: {e}")
            return self.default_cities
        finally:
            try:
                cursor.close()
                conn.close()
            except:
                pass
    
    def create_bronze_table(self, conn):
        """Create bronze table with correct schema"""
        cursor = conn.cursor()
        
        try:
            # Create BRONZE schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
            
            # Create table with lowercase column names to avoid case sensitivity issues
            create_table_sql = """
            CREATE OR REPLACE TABLE BRONZE.CURRENT_WEATHER_RAW (
                city_id VARCHAR(100),
                city_name VARCHAR(100),
                country_code VARCHAR(10),
                latitude FLOAT,
                longitude FLOAT,
                timestamp TIMESTAMP_NTZ,
                weather_main VARCHAR(50),
                weather_description VARCHAR(100),
                temperature FLOAT,
                feels_like FLOAT,
                temp_min FLOAT,
                temp_max FLOAT,
                pressure FLOAT,
                humidity FLOAT,
                wind_speed FLOAT,
                wind_deg FLOAT,
                clouds FLOAT,
                ingestion_date DATE DEFAULT CURRENT_DATE(),
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            cursor.execute(create_table_sql)
            logger.info("Bronze table created/verified")
            
        except Exception as e:
            logger.error(f"Error creating bronze table: {e}")
            raise
        finally:
            cursor.close()
    
    def fetch_weather_data(self, city_info):
        """Fetch weather data from OpenWeatherMap API"""
        try:
            params = {
                'lat': city_info['lat'],
                'lon': city_info['lon'],
                'appid': self.api_key,
                'units': 'metric'  # Get temperature in Celsius
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract relevant data
            weather_data = {
                'city_id': str(data.get('id', '')),
                'city_name': data.get('name', city_info['city']),
                'country_code': data.get('sys', {}).get('country', city_info['country']),
                'latitude': city_info['lat'],
                'longitude': city_info['lon'],
                'timestamp': datetime.fromtimestamp(data.get('dt', datetime.now().timestamp())),
                'weather_main': data.get('weather', [{}])[0].get('main', 'Unknown'),
                'weather_description': data.get('weather', [{}])[0].get('description', 'Unknown'),
                'temperature': data.get('main', {}).get('temp'),
                'feels_like': data.get('main', {}).get('feels_like'),
                'temp_min': data.get('main', {}).get('temp_min'),
                'temp_max': data.get('main', {}).get('temp_max'),
                'pressure': data.get('main', {}).get('pressure'),
                'humidity': data.get('main', {}).get('humidity'),
                'wind_speed': data.get('wind', {}).get('speed'),
                'wind_deg': data.get('wind', {}).get('deg'),
                'clouds': data.get('clouds', {}).get('all'),
                'ingestion_date': datetime.now().date()
            }
            
            return weather_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {city_info['city']}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing data for {city_info['city']}: {e}")
            return None
    
    def insert_into_snowflake(self, conn, weather_data):
        """Insert weather data into Snowflake"""
        cursor = conn.cursor()
        
        try:
            # Use parameterized query with lowercase column names
            insert_sql = """
            INSERT INTO BRONZE.CURRENT_WEATHER_RAW (
                city_id, city_name, country_code, latitude, longitude,
                timestamp, weather_main, weather_description,
                temperature, feels_like, temp_min, temp_max,
                pressure, humidity, wind_speed, wind_deg, clouds,
                ingestion_date
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s
            )
            """
            
            values = (
                weather_data['city_id'],
                weather_data['city_name'],
                weather_data['country_code'],
                weather_data['latitude'],
                weather_data['longitude'],
                weather_data['timestamp'],
                weather_data['weather_main'],
                weather_data['weather_description'],
                weather_data['temperature'],
                weather_data['feels_like'],
                weather_data['temp_min'],
                weather_data['temp_max'],
                weather_data['pressure'],
                weather_data['humidity'],
                weather_data['wind_speed'],
                weather_data['wind_deg'],
                weather_data['clouds'],
                weather_data['ingestion_date']
            )
            
            cursor.execute(insert_sql, values)
            logger.debug(f"Inserted data for {weather_data['city_name']}")
            
        except Exception as e:
            logger.error(f"Error inserting data for {weather_data['city_name']}: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_data(self):
        """Main ingestion function"""
        logger.info("Starting weather data ingestion...")
        
        try:
            # Get cities to process
            cities = self.get_cities_from_snowflake()
            logger.info(f"Processing {len(cities)} cities")
            
            # Test API connection
            if self.api_key:
                test_params = {'lat': 40.7128, 'lon': -74.0060, 'appid': self.api_key, 'units': 'metric'}
                test_response = requests.get(self.base_url, params=test_params, timeout=5)
                if test_response.status_code == 200:
                    logger.info("✅ API connection successful")
                else:
                    logger.warning(f"⚠️  API test returned status {test_response.status_code}")
            else:
                logger.warning("⚠️  No API key found, using mock data")
            
            # Connect to Snowflake
            conn = snowflake.connector.connect(**self.sf_params)
            
            # Create bronze table
            self.create_bronze_table(conn)
            
            successful_inserts = 0
            failed_inserts = 0
            
            # Process each city
            for i, city_info in enumerate(cities, 1):
                logger.info(f"Processing {i}/{len(cities)}: {city_info['city']}")
                
                # Fetch weather data
                weather_data = self.fetch_weather_data(city_info)
                
                if weather_data is None:
                    logger.warning(f"Failed to fetch data for {city_info['city']}")
                    failed_inserts += 1
                    continue
                
                # Insert into Snowflake
                try:
                    self.insert_into_snowflake(conn, weather_data)
                    successful_inserts += 1
                    logger.info(f"✓ Successfully ingested data for {city_info['city']}")
                except Exception as e:
                    logger.error(f"✗ Failed to insert data for {city_info['city']}: {e}")
                    failed_inserts += 1
            
            # Close connection
            conn.close()
            
            # Return result
            result = {
                'status': 'SUCCESS',
                'cities_processed': len(cities),
                'successful_inserts': successful_inserts,
                'failed_inserts': failed_inserts
            }
            
            logger.info(f"Ingestion completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            return {'status': 'FAILED', 'error': str(e)}


def main():
    """Main execution function"""
    import sys
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('weather_ingestion.log')
        ]
    )
    
    print("=" * 60)
    print("WEATHER DATA INGESTION - OPENWEATHERMAP TO SNOWFLAKE")
    print("=" * 60)
    
    # Run ingestion
    print("\n[Step 1] Starting ingestion process...")
    ingestor = WeatherDataIngestion()
    result = ingestor.ingest_data()
    
    # Show results
    print("\n" + "=" * 60)
    print("INGESTION RESULTS")
    print("=" * 60)
    
    status = result.get('status')
    if status == 'SUCCESS':
        print(f"✅ Status: SUCCESS")
        print(f"   Cities processed: {result.get('cities_processed', 0)}")
        print(f"   Successful inserts: {result.get('successful_inserts', 0)}")
        print(f"   Failed inserts: {result.get('failed_inserts', 0)}")
    else:
        print(f"❌ Status: FAILED")
        print(f"   Error: {result.get('error', 'Unknown error')}")
    
    print("\nCheck 'weather_ingestion.log' for detailed logs.")
    print("=" * 60)


if __name__ == "__main__":
    main()