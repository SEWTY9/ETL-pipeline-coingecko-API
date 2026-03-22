CREATE DATABASE airflow_db;

CREATE TABLE IF NOT EXISTS raw_crypto_data (
    id SERIAL PRIMARY KEY,
    raw_json JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- чистые данные
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    price_usd FLOAT,
    loaded_at TIMESTAMP
);

-- метрики
CREATE TABLE IF NOT EXISTS crypto_metrics (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    avg_price FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    records_count INT
);

-- для аналитики
CREATE TABLE IF NOT EXISTS crypto_analytics (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    price_usd FLOAT,
    loaded_at TIMESTAMP,
    daily_return FLOAT,
    ma_7 FLOAT,
    ma_30 FLOAT,
    volatility_7 FLOAT,
    volatility_30 FLOAT
);