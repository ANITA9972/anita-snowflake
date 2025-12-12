#!/usr/bin/env python3
"""
Main pipeline for Snowflake Weather Analytics
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.ingestion.weather_ingestion import WeatherDataIngestion
from scripts.transformation.data_transformation import WeatherDataTransformation
from scripts.analytics.data_analytics import WeatherAnalytics

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

class WeatherAnalyticsPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self):
        load_dotenv()
        self.start_time = datetime.now()
        logger.info(f"Pipeline started at {self.start_time}")
    
    def run_ingestion(self):
        """Run data ingestion phase"""
        logger.info("Starting data ingestion...")
        try:
            ingestor = WeatherDataIngestion()
            result = ingestor.ingest_weather_data()
            logger.info(f"Ingestion completed: {result}")
            return result
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
    
    def run_transformation(self):
        """Run data transformation phase"""
        logger.info("Starting data transformation...")
        try:
            transformer = WeatherDataTransformation()
            result = transformer.transform_data()
            logger.info(f"Transformation completed: {result}")
            return result
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def run_analytics(self):
        """Run analytics phase"""
        logger.info("Starting analytics...")
        try:
            analytics = WeatherAnalytics()
            result = analytics.generate_insights()
            logger.info(f"Analytics completed: {result}")
            return result
        except Exception as e:
            logger.error(f"Analytics failed: {e}")
            raise
    
    def log_pipeline_result(self, status, records_processed=0, error_message=None):
        """Log pipeline result to Snowflake"""
        try:
            import snowflake.connector
            from snowflake.connector import DictCursor
            
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema='UTILS'
            )
            
            cursor = conn.cursor(DictCursor)
            query = """
            INSERT INTO PIPELINE_LOGS (
                PIPELINE_NAME, STAGE, STATUS, RECORDS_PROCESSED,
                ERROR_MESSAGE, START_TIME, END_TIME
            ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """
            
            cursor.execute(query, (
                'WEATHER_ANALYTICS_PIPELINE',
                'FULL_PIPELINE',
                status,
                records_processed,
                error_message,
                self.start_time
            ))
            
            cursor.close()
            conn.close()
            logger.info("Pipeline result logged to Snowflake")
            
        except Exception as e:
            logger.error(f"Failed to log pipeline result: {e}")
    
    def run(self):
        """Run complete pipeline"""
        try:
            # Step 1: Ingestion
            ingestion_result = self.run_ingestion()
            
            # Step 2: Transformation
            transformation_result = self.run_transformation()
            
            # Step 3: Analytics
            analytics_result = self.run_analytics()
            
            # Calculate total records
            total_records = (
                ingestion_result.get('records_ingested', 0) +
                transformation_result.get('records_transformed', 0)
            )
            
            # Log success
            self.log_pipeline_result('SUCCESS', total_records)
            
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            
            logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
            logger.info(f"Total records processed: {total_records}")
            
            return {
                'status': 'SUCCESS',
                'duration_seconds': duration,
                'records_processed': total_records,
                'ingestion': ingestion_result,
                'transformation': transformation_result,
                'analytics': analytics_result
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.log_pipeline_result('FAILED', 0, str(e))
            raise

def main():
    """Main function"""
    print("=" * 60)
    print("SNOWFLAKE WEATHER ANALYTICS PIPELINE")
    print("=" * 60)
    
    try:
        pipeline = WeatherAnalyticsPipeline()
        result = pipeline.run()
        
        print("\n✅ PIPELINE COMPLETED SUCCESSFULLY")
        print(f"   Duration: {result['duration_seconds']:.2f} seconds")
        print(f"   Records: {result['records_processed']:,}")
        print(f"   Status: {result['status']}")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
