# transformation_simple.py
"""
Simple Transformation Script - Bronze to Silver
This works with the lowercase column names from the ingestion script
"""

import os
import logging
import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class SimpleWeatherTransformation:
    """Simple transformation from Bronze to Silver"""
    
    def __init__(self):
        load_dotenv()
        
        self.sf_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': 'BRONZE'
        }
    
    def create_silver_tables(self, conn):
        """Create silver tables"""
        cursor = conn.cursor()
        
        try:
            # Create SILVER schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
            
            # Silver table - matches bronze schema but with enhanced fields
            create_silver_sql = """
            CREATE OR REPLACE TABLE SILVER.CURRENT_WEATHER_CLEANED (
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
                date DATE,
                hour NUMBER,
                day_of_week VARCHAR(20),
                month NUMBER,
                season VARCHAR(20),
                hemisphere VARCHAR(20),
                climate_zone VARCHAR(50),
                temperature_category VARCHAR(50),
                humidity_category VARCHAR(50),
                comfort_index FLOAT,
                comfort_level VARCHAR(50),
                data_quality_flag VARCHAR(50),
                ingestion_date DATE,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            cursor.execute(create_silver_sql)
            logger.info("Silver table created/verified")
            
        except Exception as e:
            logger.error(f"Error creating silver table: {e}")
            raise
        finally:
            cursor.close()
    
    def get_hemisphere(self, latitude):
        """Determine hemisphere based on latitude"""
        if pd.isna(latitude):
            return 'UNKNOWN'
        elif latitude > 0:
            return 'NORTHERN'
        elif latitude < 0:
            return 'SOUTHERN'
        else:
            return 'EQUATORIAL'
    
    def get_climate_zone(self, latitude):
        """Classify climate zone based on latitude"""
        if pd.isna(latitude):
            return 'UNKNOWN'
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
        
        temp_score = max(0, 100 - abs(temp - 21) * 5)
        humidity_penalty = abs(humidity - 50) * 0.3
        wind_penalty = abs(wind_speed - 5) * 2 if not pd.isna(wind_speed) else 0
        
        comfort = temp_score - humidity_penalty - wind_penalty
        return max(0, min(100, comfort))
    
    def transform_data(self):
        """Transform data from Bronze to Silver"""
        logger.info("Starting simple transformation...")
        
        try:
            # Connect to Snowflake
            conn = snowflake.connector.connect(**self.sf_params)
            
            # Create silver tables
            self.create_silver_tables(conn)
            
            # Read data from bronze - use lowercase column names
            query = """
            SELECT 
                city_id, city_name, country_code, 
                latitude, longitude, timestamp,
                weather_main, weather_description,
                temperature, feels_like, temp_min, temp_max,
                pressure, humidity, wind_speed, wind_deg, clouds,
                ingestion_date
            FROM BRONZE.CURRENT_WEATHER_RAW
            WHERE ingestion_date >= CURRENT_DATE() - 7
            ORDER BY timestamp DESC
            """
            
            logger.info("Fetching data from bronze table...")
            df_bronze = pd.read_sql(query, conn)
            
            if df_bronze.empty:
                logger.warning("No data found in bronze table")
                conn.close()
                return {'status': 'WARNING', 'message': 'No bronze data'}
            
            logger.info(f"Found {len(df_bronze)} rows in bronze table")
            
            # Convert column names to uppercase for consistency
            df_bronze.columns = [col.upper() for col in df_bronze.columns]
            
            # Create a copy for transformation
            df_silver = df_bronze.copy()
            
            # Ensure proper data types
            numeric_cols = ['TEMPERATURE', 'FEELS_LIKE', 'TEMP_MIN', 'TEMP_MAX', 
                           'PRESSURE', 'HUMIDITY', 'WIND_SPEED', 'WIND_DEG', 
                           'CLOUDS', 'LATITUDE', 'LONGITUDE']
            
            for col in numeric_cols:
                if col in df_silver.columns:
                    df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')
            
            # Fill missing values
            for col in ['WIND_SPEED', 'WIND_DEG', 'CLOUDS']:
                if col in df_silver.columns:
                    df_silver[col] = df_silver[col].fillna(0)
            
            # Data quality flag
            df_silver['DATA_QUALITY_FLAG'] = 'VALID'
            if 'TEMPERATURE' in df_silver.columns:
                mask = (df_silver['TEMPERATURE'] < -50) | (df_silver['TEMPERATURE'] > 60)
                df_silver.loc[mask, 'DATA_QUALITY_FLAG'] = 'EXTREME_TEMPERATURE'
            
            # Keep only valid data
            df_valid = df_silver[df_silver['DATA_QUALITY_FLAG'] == 'VALID'].copy()
            
            if df_valid.empty:
                logger.warning("No valid data after quality filtering")
                conn.close()
                return {'status': 'WARNING', 'message': 'No valid data'}
            
            logger.info(f"{len(df_valid)} rows after quality filtering")
            
            # Temporal features
            df_valid['TIMESTAMP'] = pd.to_datetime(df_valid['TIMESTAMP'], errors='coerce')
            df_valid['DATE'] = df_valid['TIMESTAMP'].dt.date
            df_valid['HOUR'] = df_valid['TIMESTAMP'].dt.hour
            df_valid['DAY_OF_WEEK'] = df_valid['TIMESTAMP'].dt.day_name()
            df_valid['MONTH'] = df_valid['TIMESTAMP'].dt.month
            
            # Season
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
            
            # Geographic features
            df_valid['HEMISPHERE'] = df_valid['LATITUDE'].apply(self.get_hemisphere)
            df_valid['CLIMATE_ZONE'] = df_valid['LATITUDE'].apply(self.get_climate_zone)
            
            # Temperature category
            def categorize_temperature(temp):
                if pd.isna(temp):
                    return 'UNKNOWN'
                elif temp < 0:
                    return 'FREEZING'
                elif temp < 10:
                    return 'COLD'
                elif temp < 20:
                    return 'COOL'
                elif temp < 30:
                    return 'WARM'
                else:
                    return 'HOT'
            
            df_valid['TEMPERATURE_CATEGORY'] = df_valid['TEMPERATURE'].apply(categorize_temperature)
            
            # Humidity category
            def categorize_humidity(humidity):
                if pd.isna(humidity):
                    return 'UNKNOWN'
                elif humidity < 30:
                    return 'DRY'
                elif humidity < 60:
                    return 'COMFORTABLE'
                elif humidity < 80:
                    return 'HUMID'
                else:
                    return 'VERY_HUMID'
            
            df_valid['HUMIDITY_CATEGORY'] = df_valid['HUMIDITY'].apply(categorize_humidity)
            
            # Comfort index
            df_valid['COMFORT_INDEX'] = df_valid.apply(
                lambda row: self.calculate_comfort_index(
                    row.get('TEMPERATURE'),
                    row.get('HUMIDITY'),
                    row.get('WIND_SPEED', 0)
                ), axis=1
            )
            
            # Comfort level
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
            
            # Ensure all required columns
            silver_columns = [
                'CITY_ID', 'CITY_NAME', 'COUNTRY_CODE', 'LATITUDE', 'LONGITUDE',
                'TIMESTAMP', 'WEATHER_MAIN', 'WEATHER_DESCRIPTION',
                'TEMPERATURE', 'FEELS_LIKE', 'TEMP_MIN', 'TEMP_MAX',
                'PRESSURE', 'HUMIDITY', 'WIND_SPEED', 'WIND_DEG',
                'CLOUDS', 'DATE', 'HOUR', 'DAY_OF_WEEK', 'MONTH',
                'SEASON', 'HEMISPHERE', 'CLIMATE_ZONE', 'TEMPERATURE_CATEGORY',
                'HUMIDITY_CATEGORY', 'COMFORT_INDEX', 'COMFORT_LEVEL',
                'DATA_QUALITY_FLAG', 'INGESTION_DATE'
            ]
            
            for col in silver_columns:
                if col not in df_valid.columns:
                    df_valid[col] = None
            
            df_valid = df_valid[silver_columns]
            
            # Write to silver table
            logger.info(f"Writing {len(df_valid)} rows to SILVER.CURRENT_WEATHER_CLEANED")
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_valid,
                table_name='CURRENT_WEATHER_CLEANED',
                schema='SILVER',
                database=os.getenv('SNOWFLAKE_DATABASE'),
                quote_identifiers=False
            )
            
            conn.close()
            
            if success:
                result = {
                    'status': 'SUCCESS',
                    'bronze_rows': len(df_bronze),
                    'silver_rows': nrows
                }
                logger.info(f"Transformation successful: {result}")
                return result
            else:
                logger.error("Failed to write to silver table")
                return {'status': 'FAILED', 'error': 'Silver write failed'}
                
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'FAILED', 'error': str(e)}


def check_bronze_data():
    """Check what's in the bronze table"""
    load_dotenv()
    
    sf_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': 'PUBLIC'
    }
    
    try:
        conn = snowflake.connector.connect(**sf_params)
        cursor = conn.cursor()
        
        print("=" * 60)
        print("CHECKING BRONZE TABLE DATA")
        print("=" * 60)
        
        # Check table exists
        cursor.execute("SHOW TABLES LIKE 'CURRENT_WEATHER_RAW' IN SCHEMA BRONZE")
        tables = cursor.fetchall()
        
        if not tables:
            print("❌ BRONZE.CURRENT_WEATHER_RAW table does not exist!")
            cursor.close()
            conn.close()
            return False
        
        # Show columns
        cursor.execute("DESCRIBE TABLE BRONZE.CURRENT_WEATHER_RAW")
        columns = cursor.fetchall()
        
        print(f"\n📋 Table columns ({len(columns)}):")
        for col in columns:
            print(f"   - {col[0]}: {col[1]}")
        
        # Count rows
        cursor.execute("SELECT COUNT(*) FROM BRONZE.CURRENT_WEATHER_RAW")
        count = cursor.fetchone()[0]
        print(f"\n📊 Total rows: {count}")
        
        if count > 0:
            # Show sample data
            cursor.execute("""
            SELECT 
                city_name, country_code, latitude, longitude,
                temperature, humidity, weather_main,
                timestamp, ingestion_date
            FROM BRONZE.CURRENT_WEATHER_RAW 
            ORDER BY timestamp DESC 
            LIMIT 3
            """)
            
            samples = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            
            print(f"\n🔍 Sample data (3 most recent):")
            for i, sample in enumerate(samples, 1):
                print(f"\n   Record {i}:")
                for name, value in zip(col_names, sample):
                    print(f"     {name}: {value}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error checking bronze table: {e}")
        return False


def main():
    """Main execution"""
    import sys
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('simple_transformation.log')
        ]
    )
    
    print("\n" + "="*60)
    print("SIMPLE WEATHER TRANSFORMATION")
    print("="*60)
    
    # Check bronze table first
    print("\n[Step 1] Checking bronze table...")
    bronze_ok = check_bronze_data()
    
    if not bronze_ok:
        print("\n❌ Cannot proceed: Bronze table issues detected")
        print("\nPlease run the ingestion script first:")
        print("python ingestion_fixed.py")
        return
    
    # Run transformation
    print("\n[Step 2] Running transformation...")
    transformer = SimpleWeatherTransformation()
    result = transformer.transform_data()
    
    # Show results
    print("\n" + "="*60)
    print("TRANSFORMATION RESULTS")
    print("="*60)
    
    status = result.get('status')
    if status == 'SUCCESS':
        print(f"✅ Status: SUCCESS")
        print(f"   Bronze rows processed: {result.get('bronze_rows', 0)}")
        print(f"   Silver rows written: {result.get('silver_rows', 0)}")
    elif status == 'WARNING':
        print(f"⚠️  Status: WARNING")
        print(f"   Message: {result.get('message', 'Unknown warning')}")
    else:
        print(f"❌ Status: FAILED")
        print(f"   Error: {result.get('error', 'Unknown error')}")
    
    print("\nCheck 'simple_transformation.log' for detailed logs.")
    print("="*60)


if __name__ == "__main__":
    main()