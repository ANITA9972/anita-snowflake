#!/bin/bash
# Start all services for Snowflake Weather Analytics

echo "Starting Snowflake Weather Analytics Services..."

# Start Jupyter notebook
echo "Starting Jupyter Notebook..."
cd /home/davidwaga/data_projects/snowflake-weather-analytics
nohup /home/davidwaga/data_projects/snowflake-weather-analytics/venv/bin/jupyter notebook --no-browser --port=8888 > logs/jupyter.log 2>&1 &
echo "Jupyter started on port 8888"

# Start Streamlit dashboard
echo "Starting Streamlit Dashboard..."
cd /home/davidwaga/data_projects/snowflake-weather-analytics
nohup /home/davidwaga/data_projects/snowflake-weather-analytics/venv/bin/streamlit run scripts/dashboard/weather_dashboard.py --server.port 8501 --server.address 0.0.0.0 > logs/streamlit.log 2>&1 &
echo "Streamlit started on port 8501"

# Start systemd service
echo "Starting pipeline service..."
sudo systemctl start snowflake-weather.service
sudo systemctl enable snowflake-weather.service

echo "All services started!"
echo "Jupyter: http://localhost:8888"
echo "Streamlit: http://localhost:8501"
echo "Check logs: tail -f logs/pipeline.log"
