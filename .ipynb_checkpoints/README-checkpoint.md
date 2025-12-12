# Snowflake Weather Analytics Project

## Project Overview
End-to-end weather analytics pipeline using Snowflake and OpenWeatherMap API.

## Directory Structure
- scripts/     : Python scripts for data pipeline
- sql/         : SQL scripts and queries
- notebooks/   : Jupyter notebooks for exploration
- config/      : Configuration files
- data/        : Local data storage (if needed)
- logs/        : Log files
- tests/       : Test scripts

## Setup Instructions
1. Configure Snowflake connection in config/snowflake.cfg
2. Set up OpenWeatherMap API key
3. Run database setup: sql/setup_database.sql
4. Execute pipeline: python scripts/main.py
