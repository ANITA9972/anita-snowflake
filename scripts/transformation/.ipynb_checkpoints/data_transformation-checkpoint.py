"""
Data Transformation from Bronze to Silver Layer
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class WeatherDataTransformation:
    """Transform weather data from Bronze to Silver layer"""
    
    def __init__(self):
        load_dotenv()
        
        self.sf_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': 'SILVER'
        }
    
    def get_hemisphere(self, latitude):
        """Determine hemisphere based on latitude"""
        if latitude > 0:
            return 'NORTHERN'
        elif latitude < 0:
            return 'SOUTHERN'
        else:
            return 'EQUATORIAL'
    
    def get_climate_zone(self, latitude):
        """Classify climate zone based on latitude"""
        abs_lat = abs(latitude)
        if abs_lat <= 23.5:
            return 'TROPICAL'
        elif abs_lat <= 35:
            return 'SUBTROPICAL'
        elif abs_lat <= 55:
            return 'TEMPERATE'
        else:
            return 'POLAR'
    
    def calculate_comfort_index(self, temp, humidity, wind_speed):
        """Calculate thermal comfort index"""
        if pd.isna(temp) or pd.isna(humidity):
            return None
        
        # Base comfort (0-100 scale)
        # Ideal temperature: 21°C
        temp_score = max(0, 100 - abs(temp - 21) * 5)
        
        # Humidity adjustment (ideal: 50%)
        humidity_penalty = abs(humidity - 50) * 0.3
        
        # Wind adjustment (moderate wind is comfortable)
        wind_penalty = abs(wind_speed - 5) * 2 if not pd.isna(wind_speed) else 0
        
        comfort = temp_score - humidity_penalty - wind_penalty
        return max(0, min(100, comfort))
    
    def create_silver_tables(self, conn):
        """Create silver layer tables"""
        cursor = conn.cursor()
        
        # Current weather cleaned
        create_current_sql = """
        CREATE TABLE IF NOT EXISTS CURRENT_WEATHER_CLEANED (
            CITY_ID NUMBER,
            CITY_NAME VARCHAR(100),
            COUNTRY_CODE VARCHAR(10),
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            TIMESTAMP TIMESTAMP_NTZ,
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
            DATE DATE,
            HOUR NUMBER,
            DAY_OF_WEEK VARCHAR(20),
            MONTH NUMBER,
            SEASON VARCHAR(20),
            HEMISPHERE VARCHAR(20),
            CLIMATE_ZONE VARCHAR(50),
            TEMPERATURE_CATEGORY VARCHAR(50),
            HUMIDITY_CATEGORY VARCHAR(50),
            COMFORT_INDEX FLOAT,
            COMFORT_LEVEL VARCHAR(50),
            WEATHER_SEVERITY_SCORE FLOAT,
            SEVERITY_LEVEL VARCHAR(50),
            DATA_QUALITY_FLAG VARCHAR(50),
            INGESTION_DATE DATE
        )
        """
        
        # Daily aggregations
        create_daily_sql = """
        CREATE TABLE IF NOT EXISTS WEATHER_DAILY (
            CITY_ID NUMBER,
            CITY_NAME VARCHAR(100),
            COUNTRY_CODE VARCHAR(10),
            DATE DATE,
            CLIMATE_ZONE VARCHAR(50),
            HEMISPHERE VARCHAR(20),
            HOURLY_READINGS NUMBER,
            AVG_TEMPERATURE FLOAT,
            MIN_TEMPERATURE FLOAT,
            MAX_TEMPERATURE FLOAT,
            TEMPERATURE_VARIABILITY FLOAT,
            AVG_HUMIDITY FLOAT,
            MIN_HUMIDITY FLOAT,
            MAX_HUMIDITY FLOAT,
            AVG_WIND_SPEED FLOAT,
            MAX_WIND_SPEED FLOAT,
            AVG_PRESSURE FLOAT,
            AVG_COMFORT_INDEX FLOAT,
            DOMINANT_WEATHER VARCHAR(50),
            WEATHER_CHANGES_COUNT NUMBER,
            DAILY_TEMP_RANGE FLOAT,
            TEMPERATURE_STABILITY VARCHAR(50),
            COMFORT_CATEGORY VARCHAR(50),
            LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        cursor.execute(create_current_sql)
        cursor.execute(create_daily_sql)
        cursor.close()
        logger.info("Silver tables created/verified")
    
    def transform_data(self) -> Dict:
        """Transform data from Bronze to Silver layer"""
        logger.info("Starting data transformation...")
        
        try:
            conn = snowflake.connector.connect(**self.sf_params)
            
            # Create tables
            self.create_silver_tables(conn)
            
            # Read bronze data
            query = """
            SELECT 
                CITY_ID, CITY_NAME, COUNTRY_CODE, LATITUDE, LONGITUDE,
                TIMESTAMP, WEATHER_MAIN, WEATHER_DESCRIPTION,
                TEMPERATURE, FEELS_LIKE, TEMP_MIN, TEMP_MAX,
                PRESSURE, HUMIDITY, WIND_SPEED, WIND_DEG,
                CLOUD_COVERAGE, INGESTION_DATE
            FROM BRONZE.CURRENT_WEATHER_RAW
            WHERE INGESTION_DATE >= CURRENT_DATE() - 1
            ORDER BY TIMESTAMP DESC
            """
            
            df_bronze = pd.read_sql(query, conn)
            
            if df_bronze.empty:
                logger.warning("No bronze data found for transformation")
                conn.close()
                return {'status': 'WARNING', 'message': 'No data to transform'}
            
            logger.info(f"Loaded {len(df_bronze)} records from bronze layer")
            
            # Data cleaning
            df_clean = df_bronze.copy()
            
            # Fill missing values
            df_clean['WIND_SPEED'] = df_clean['WIND_SPEED'].fillna(0)
            df_clean['WIND_DEG'] = df_clean['WIND_DEG'].fillna(0)
            df_clean['CLOUD_COVERAGE'] = df_clean['CLOUD_COVERAGE'].fillna(0)
            
            # Add data quality flag
            df_clean['DATA_QUALITY_FLAG'] = 'VALID'
            df_clean.loc[
                (df_clean['TEMPERATURE'] < -50) | (df_clean['TEMPERATURE'] > 60),
                'DATA_QUALITY_FLAG'
            ] = 'EXTREME_TEMPERATURE'
            
            # Filter valid data
            df_valid = df_clean[df_clean['DATA_QUALITY_FLAG'] == 'VALID']
            
            # Add temporal features
            df_valid['DATE'] = pd.to_datetime(df_valid['TIMESTAMP']).dt.date
            df_valid['HOUR'] = pd.to_datetime(df_valid['TIMESTAMP']).dt.hour
            df_valid['DAY_OF_WEEK'] = pd.to_datetime(df_valid['TIMESTAMP']).dt.day_name()
            df_valid['MONTH'] = pd.to_datetime(df_valid['TIMESTAMP']).dt.month
            
            # Add season
            def get_season(month):
                if month in [12, 1, 2]:
                    return 'WINTER'
                elif month in [3, 4, 5]:
                    return 'SPRING'
                elif month in [6, 7, 8]:
                    return 'SUMMER'
                else:
                    return 'AUTUMN'
            
            df_valid['SEASON'] = df_valid['MONTH'].apply(get_season)
            
            # Add geographic features
            df_valid['HEMISPHERE'] = df_valid['LATITUDE'].apply(self.get_hemisphere)
            df_valid['CLIMATE_ZONE'] = df_valid['LATITUDE'].apply(self.get_climate_zone)
            
            # Add weather categories
            def categorize_temperature(temp):
                if temp < 0:
                    return 'FREEZING'
                elif temp < 10:
                    return 'COLD'
                elif temp < 20:
                    return 'COOL'
                elif temp < 30:
                    return 'WARM'
                else:
                    return 'HOT'
            
            def categorize_humidity(humidity):
                if humidity < 30:
                    return 'DRY'
                elif humidity < 60:
                    return 'COMFORTABLE'
                elif humidity < 80:
                    return 'HUMID'
                else:
                    return 'VERY_HUMID'
            
            df_valid['TEMPERATURE_CATEGORY'] = df_valid['TEMPERATURE'].apply(categorize_temperature)
            df_valid['HUMIDITY_CATEGORY'] = df_valid['HUMIDITY'].apply(categorize_humidity)
            
            # Calculate comfort index
            df_valid['COMFORT_INDEX'] = df_valid.apply(
                lambda row: self.calculate_comfort_index(
                    row['TEMPERATURE'], 
                    row['HUMIDITY'], 
                    row['WIND_SPEED']
                ), axis=1
            )
            
            # Add comfort level
            def categorize_comfort(comfort):
                if pd.isna(comfort):
                    return 'UNKNOWN'
                elif comfort >= 80:
                    return 'VERY_COMFORTABLE'
                elif comfort >= 60:
                    return 'COMFORTABLE'
                elif comfort >= 40:
                    return 'MODERATE'
                elif comfort >= 20:
                    return 'UNCOMFORTABLE'
                else:
                    return 'VERY_UNCOMFORTABLE'
            
            df_valid['COMFORT_LEVEL'] = df_valid['COMFORT_INDEX'].apply(categorize_comfort)
            
            # Calculate weather severity
            df_valid['WEATHER_SEVERITY_SCORE'] = (
                abs(df_valid['TEMPERATURE'] - 21) / 40 * 0.4 +
                abs(df_valid['WIND_SPEED'] - 5) / 20 * 0.3 +
                abs(df_valid['HUMIDITY'] - 50) / 100 * 0.2 +
                (100 - df_valid['CLOUD_COVERAGE']) / 100 * 0.1
            )
            
            # Add severity level
            def categorize_severity(score):
                if score < 0.2:
                    return 'MILD'
                elif score < 0.4:
                    return 'MODERATE'
                elif score < 0.6:
                    return 'SEVERE'
                else:
                    return 'EXTREME'
            
            df_valid['SEVERITY_LEVEL'] = df_valid['WEATHER_SEVERITY_SCORE'].apply(categorize_severity)
            
            # Write to silver table
            success, nchunks, nrows, _ = pd_writer(
                df=df_valid,
                con=conn,
                name='CURRENT_WEATHER_CLEANED',
                schema='SILVER',
                if_exists='append'
            )
            
            if success:
                logger.info(f"Successfully wrote {nrows} rows to silver layer")
                
                # Create daily aggregations
                self.create_daily_aggregations(conn, df_valid)
                
                conn.close()
                
                return {
                    'status': 'SUCCESS',
                    'records_transformed': len(df_valid),
                    'silver_rows_written': nrows
                }
            else:
                logger.error("Failed to write to silver layer")
                conn.close()
                return {'status': 'FAILED', 'error': 'Silver write failed'}
                
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            return {'status': 'FAILED', 'error': str(e)}
    
    def create_daily_aggregations(self, conn, df_silver):
        """Create daily aggregated data"""
        try:
            # Group by city and date
            daily_agg = df_silver.groupby(['CITY_ID', 'CITY_NAME', 'COUNTRY_CODE', 'DATE', 'CLIMATE_ZONE', 'HEMISPHERE']).agg({
                'TEMPERATURE': ['count', 'mean', 'min', 'max', 'std'],
                'HUMIDITY': ['mean', 'min', 'max'],
                'WIND_SPEED': ['mean', 'max'],
                'PRESSURE': ['mean'],
                'COMFORT_INDEX': ['mean'],
                'WEATHER_MAIN': lambda x: x.mode()[0] if not x.mode().empty else 'UNKNOWN',
                'WEATHER_MAIN': 'nunique'
            }).reset_index()
            
            # Flatten column names
            daily_agg.columns = [
                'CITY_ID', 'CITY_NAME', 'COUNTRY_CODE', 'DATE', 'CLIMATE_ZONE', 'HEMISPHERE',
                'HOURLY_READINGS', 'AVG_TEMPERATURE', 'MIN_TEMPERATURE', 'MAX_TEMPERATURE',
                'TEMPERATURE_VARIABILITY', 'AVG_HUMIDITY', 'MIN_HUMIDITY', 'MAX_HUMIDITY',
                'AVG_WIND_SPEED', 'MAX_WIND_SPEED', 'AVG_PRESSURE', 'AVG_COMFORT_INDEX',
                'DOMINANT_WEATHER', 'WEATHER_CHANGES_COUNT'
            ]
            
            # Calculate additional metrics
            daily_agg['DAILY_TEMP_RANGE'] = daily_agg['MAX_TEMPERATURE'] - daily_agg['MIN_TEMPERATURE']
            
            def categorize_stability(variability):
                if pd.isna(variability):
                    return 'UNKNOWN'
                elif variability < 2:
                    return 'STABLE'
                elif variability < 5:
                    return 'MODERATE'
                else:
                    return 'VARIABLE'
            
            daily_agg['TEMPERATURE_STABILITY'] = daily_agg['TEMPERATURE_VARIABILITY'].apply(categorize_stability)
            
            def categorize_daily_comfort(comfort):
                if pd.isna(comfort):
                    return 'UNKNOWN'
                elif comfort >= 70:
                    return 'COMFORTABLE'
                elif comfort >= 50:
                    return 'MODERATE'
                else:
                    return 'UNCOMFORTABLE'
            
            daily_agg['COMFORT_CATEGORY'] = daily_agg['AVG_COMFORT_INDEX'].apply(categorize_daily_comfort)
            
            # Write to daily table
            success, nchunks, nrows, _ = pd_writer(
                df=daily_agg,
                con=conn,
                name='WEATHER_DAILY',
                schema='SILVER',
                if_exists='append'
            )
            
            if success:
                logger.info(f"Created {len(daily_agg)} daily aggregated records")
            else:
                logger.error("Failed to create daily aggregations")
                
        except Exception as e:
            logger.error(f"Daily aggregation failed: {e}")

if __name__ == "__main__":
    # Test transformation
    logging.basicConfig(level=logging.INFO)
    transformer = WeatherDataTransformation()
    result = transformer.transform_data()
    print(f"Transformation result: {result}")
