# My First ETL Project

A cryptocurrency ETL pipeline built with Apache Airflow, PostgreSQL, and Python.

## Overview

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for cryptocurrency data:

- **Extract**: Fetches real-time cryptocurrency prices from CoinGecko API
- **Transform**: Processes raw JSON data into structured format
- **Load**: Stores data in PostgreSQL database with bronze (raw) and gold (processed) layers
- **Analytics**: Calculates metrics and performs pandas-based analysis

## Architecture

- **Airflow**: Orchestrates the ETL pipeline with scheduled runs every 12 hours
- **PostgreSQL**: Stores raw data, processed prices, metrics, and analytics results
- **Docker**: Containerized environment for easy deployment

## Database Schema

- `raw_crypto_data`: Raw JSON from API
- `crypto_prices`: Processed price data
- `crypto_metrics`: Aggregated statistics per coin
- `crypto_analytics`: Pandas-calculated metrics (returns, moving averages, volatility)

## Setup

1. **Prerequisites**:
   - Docker and Docker Compose
   - Git

2. **Clone and run**:
   ```bash
   git clone <your-repo-url>
   cd my-first-ETL
   docker-compose up -d
   ```

3. **Access services**:
   - Airflow UI: http://localhost:8080 (admin/admin)
   - pgAdmin: http://localhost:5050 (admin@admin.com / admin)
   - PostgreSQL: localhost:5432 (postgres/postgres)

## Usage

1. Enable the DAG `crypto_etl_pipeline` in Airflow UI
2. Trigger manual run or wait for scheduled execution (every 12 hours)
3. Monitor task execution and view logs
4. Check results in pgAdmin or connect to database

## Project Structure

```
.
├── dags/
│   └── crypto_etl_dag.py      # Airflow DAG definition
├── db/
│   └── init.sql               # Database schema
├── etl/
│   ├── main.py                # ETL functions
│   ├── analytics.py           # Analytics functions
│   ├── requirements.txt       # Python dependencies
│   └── Dockerfile             # ETL container
├── docker-compose.yml         # Services configuration
├── .env                       # Environment variables
└── README.md                  # This file
```

## Technologies Used

- Python 3.11
- Apache Airflow 2.9.1
- PostgreSQL 15
- SQLAlchemy
- Pandas
- Requests
- Docker & Docker Compose

## Features

- Type hints for better code readability
- Comprehensive logging
- Error handling and retries
- Health checks for database connectivity
- Modular architecture
- Containerized deployment

## Contributing

Feel free to open issues or submit pull requests!