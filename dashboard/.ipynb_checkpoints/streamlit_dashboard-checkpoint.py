# simple_dashboard.py
"""
Simple Weather Dashboard - Working Version
"""
!pip install streamlit
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv

# Page configuration
st.set_page_config(
    page_title="Weather Analytics Dashboard",
    page_icon="🌤️",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Title
st.title("🌤️ Weather Analytics Dashboard")
st.markdown("Real-time analysis of weather data from multiple cities")

# Custom CSS to hide warnings
hide_streamlit_style = """
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    div[data-testid="stToolbar"] {display:none;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Database connection function
def get_snowflake_data(query):
    """Get data from Snowflake using pandas"""
    try:
        import snowflake.connector
        
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='SILVER'
        )
        
        # Use cursor to fetch data safely
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        # Close connections
        cursor.close()
        conn.close()
        
        return df
        
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# Check if table exists
def check_table_exists():
    """Check if the silver table exists"""
    try:
        query = """
        SELECT COUNT(*) as count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'SILVER' 
        AND TABLE_NAME = 'CURRENT_WEATHER_CLEANED'
        """
        result = get_snowflake_data(query)
        return result['COUNT'][0] > 0 if not result.empty else False
    except:
        return False

# Sidebar filters
st.sidebar.title("Filters")

# Check table existence
if not check_table_exists():
    st.error("""
    ❌ SILVER.CURRENT_WEATHER_CLEANED table not found!
    
    Please run the transformation script first:
    ```bash
    python transformation_simple.py
    ```
    """)
    st.stop()

# Get date range
date_query = """
SELECT 
    MIN(DATE) as min_date, 
    MAX(DATE) as max_date,
    COUNT(*) as total_rows
FROM SILVER.CURRENT_WEATHER_CLEANED
"""
date_info = get_snowflake_data(date_query)

if date_info.empty:
    st.warning("No data found in the table")
    st.stop()

min_date = date_info['MIN_DATE'][0]
max_date = date_info['MAX_DATE'][0]
total_rows = date_info['TOTAL_ROWS'][0]

st.sidebar.write(f"**Data Info:**")
st.sidebar.write(f"- Total rows: {total_rows:,}")
st.sidebar.write(f"- Date range: {min_date} to {max_date}")

# Date filter
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# Get distinct cities
cities_query = """
SELECT DISTINCT CITY_NAME 
FROM SILVER.CURRENT_WEATHER_CLEANED 
ORDER BY CITY_NAME
"""
cities_df = get_snowflake_data(cities_query)
cities = cities_df['CITY_NAME'].tolist() if not cities_df.empty else []

# City filter
selected_cities = st.sidebar.multiselect(
    "Select Cities",
    options=cities,
    default=cities[:min(5, len(cities))] if cities else []
)

# Build query based on filters
def build_query():
    """Build SQL query with filters"""
    
    conditions = []
    
    # Date range
    if len(date_range) == 2:
        conditions.append(f"DATE >= '{date_range[0]}' AND DATE <= '{date_range[1]}'")
    
    # Cities
    if selected_cities:
        cities_str = "', '".join(selected_cities)
        conditions.append(f"CITY_NAME IN ('{cities_str}')")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
    SELECT 
        CITY_NAME,
        DATE,
        TIMESTAMP,
        CAST(TEMPERATURE AS FLOAT) as TEMPERATURE,
        CAST(HUMIDITY AS FLOAT) as HUMIDITY,
        CAST(WIND_SPEED AS FLOAT) as WIND_SPEED,
        CAST(PRESSURE AS FLOAT) as PRESSURE,
        WEATHER_MAIN,
        CLIMATE_ZONE,
        HEMISPHERE,
        COMFORT_LEVEL,
        COMFORT_INDEX
    FROM SILVER.CURRENT_WEATHER_CLEANED
    WHERE {where_clause}
    ORDER BY TIMESTAMP DESC
    LIMIT 1000
    """
    
    return query

# Load data
if len(date_range) == 2:
    query = build_query()
    df = get_snowflake_data(query)
else:
    st.warning("Please select a date range")
    df = pd.DataFrame()

# Display data
if not df.empty:
    # Convert columns to numeric where needed
    numeric_cols = ['TEMPERATURE', 'HUMIDITY', 'WIND_SPEED', 'PRESSURE', 'COMFORT_INDEX']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # KPI Metrics
    st.subheader("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_temp = df['TEMPERATURE'].mean() if 'TEMPERATURE' in df.columns else 0
        st.metric("🌡️ Avg Temperature", f"{avg_temp:.1f}°C")
    
    with col2:
        avg_humidity = df['HUMIDITY'].mean() if 'HUMIDITY' in df.columns else 0
        st.metric("💧 Avg Humidity", f"{avg_humidity:.1f}%")
    
    with col3:
        avg_wind = df['WIND_SPEED'].mean() if 'WIND_SPEED' in df.columns else 0
        st.metric("💨 Avg Wind Speed", f"{avg_wind:.1f} m/s")
    
    with col4:
        cities_count = df['CITY_NAME'].nunique() if 'CITY_NAME' in df.columns else 0
        st.metric("🏙️ Cities", cities_count)
    
    # CHART 1: Temperature Trends
    st.subheader("📈 Temperature Trends by City")
    
    if 'DATE' in df.columns and 'TEMPERATURE' in df.columns and 'CITY_NAME' in df.columns:
        # Group by date and city
        temp_trend = df.groupby(['DATE', 'CITY_NAME'])['TEMPERATURE'].mean().reset_index()
        
        fig1 = px.line(
            temp_trend,
            x='DATE',
            y='TEMPERATURE',
            color='CITY_NAME',
            title='Daily Average Temperature',
            labels={'TEMPERATURE': 'Temperature (°C)', 'DATE': 'Date'}
        )
        
        st.plotly_chart(fig1, use_container_width=True)
    
    # CHART 2: Weather Conditions
    st.subheader("🌤️ Weather Conditions Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'WEATHER_MAIN' in df.columns:
            weather_counts = df['WEATHER_MAIN'].value_counts().reset_index()
            weather_counts.columns = ['Weather', 'Count']
            
            fig2 = px.pie(
                weather_counts,
                values='Count',
                names='Weather',
                title='Weather Conditions',
                hole=0.3
            )
            st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        if 'TEMPERATURE' in df.columns and 'HUMIDITY' in df.columns:
            fig3 = px.scatter(
                df.head(200),
                x='TEMPERATURE',
                y='HUMIDITY',
                color='CITY_NAME' if 'CITY_NAME' in df.columns else None,
                title='Temperature vs Humidity',
                labels={'TEMPERATURE': 'Temperature (°C)', 'HUMIDITY': 'Humidity (%)'}
            )
            st.plotly_chart(fig3, use_container_width=True)
    
    # CHART 3: Climate Zone Analysis
    st.subheader("🌍 Analysis by Climate Zone")
    
    if 'CLIMATE_ZONE' in df.columns and 'TEMPERATURE' in df.columns:
        climate_stats = df.groupby('CLIMATE_ZONE').agg({
            'TEMPERATURE': 'mean',
            'HUMIDITY': 'mean'
        }).reset_index()
        
        fig4 = go.Figure(data=[
            go.Bar(name='Avg Temp', x=climate_stats['CLIMATE_ZONE'], y=climate_stats['TEMPERATURE'], marker_color='coral'),
            go.Bar(name='Avg Humidity', x=climate_stats['CLIMATE_ZONE'], y=climate_stats['HUMIDITY'], marker_color='lightblue')
        ])
        
        fig4.update_layout(
            title='Average Temperature & Humidity by Climate Zone',
            barmode='group'
        )
        
        st.plotly_chart(fig4, use_container_width=True)
    
    # Data Table
    st.subheader("📋 Recent Data")
    
    # Select columns to display
    display_cols = []
    for col in ['CITY_NAME', 'DATE', 'TEMPERATURE', 'HUMIDITY', 'WEATHER_MAIN', 'CLIMATE_ZONE', 'COMFORT_LEVEL']:
        if col in df.columns:
            display_cols.append(col)
    
    if display_cols:
        st.dataframe(df[display_cols].head(20), use_container_width=True)
    
    # Export options
    st.subheader("💾 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="weather_data.csv",
            mime="text/csv"
        )
    
    with col2:
        summary = f"""
        Weather Data Summary
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Total Records: {len(df):,}
        Cities: {df['CITY_NAME'].nunique() if 'CITY_NAME' in df.columns else 0}
        Date Range: {df['DATE'].min() if 'DATE' in df.columns else 'N/A'} to {df['DATE'].max() if 'DATE' in df.columns else 'N/A'}
        Avg Temperature: {avg_temp:.1f}°C
        Avg Humidity: {avg_humidity:.1f}%
        """
        
        st.download_button(
            label="Download Summary",
            data=summary,
            file_name="weather_summary.txt",
            mime="text/plain"
        )
    
else:
    if len(date_range) == 2:
        st.info("No data found for the selected filters")

# Footer
st.markdown("---")
st.markdown("📊 **Weather Analytics Dashboard** | Powered by Snowflake ❄️ & Streamlit")

# Debug info (collapsed by default)
with st.expander("Debug Information"):
    st.write("### Query Executed")
    if len(date_range) == 2:
        st.code(build_query(), language="sql")
    
    st.write("### Data Info")
    st.write(f"- Rows: {len(df)}")
    st.write(f"- Columns: {len(df.columns) if not df.empty else 0}")
    
    if not df.empty:
        st.write("### Column Types")
        for col in df.columns:
            st.write(f"- {col}: {df[col].dtype}")