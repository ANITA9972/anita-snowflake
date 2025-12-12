#!/usr/bin/env python3
"""
Test Snowflake connection
"""

import os
from dotenv import load_dotenv
import snowflake.connector

def test_snowflake_connection():
    """Test connection to Snowflake"""
    load_dotenv()
    
    try:
        # Get credentials from environment variables
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE')
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        role = os.getenv('SNOWFLAKE_ROLE')
        
        print(f"Connecting to Snowflake...")
        print(f"Account: {account}")
        print(f"User: {user}")
        print(f"Warehouse: {warehouse}")
        
        # Establish connection
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER()")
        result = cursor.fetchone()
        
        print("\n✅ Connection successful!")
        print(f"Snowflake Version: {result[0]}")
        print(f"Account: {result[1]}")
        print(f"User: {result[2]}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_snowflake_connection()
