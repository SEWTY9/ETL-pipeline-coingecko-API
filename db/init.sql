CREATE DATABASE airflow_db;

-- ============================================================
-- BRONZE LAYER: сырые данные от API как есть (JSONB)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_crypto_data (
    id SERIAL PRIMARY KEY,
    raw_json JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SILVER LAYER: очищенные, провалидированные данные
-- - NUMERIC вместо FLOAT для точности
-- - CHECK на положительную цену
-- - UNIQUE (coin, extracted_at) защищает от дублей при ретраях
-- ============================================================
CREATE TABLE IF NOT EXISTS crypto_prices_silver (
    id SERIAL PRIMARY KEY,
    coin TEXT NOT NULL,
    price_usd NUMERIC(20, 8) NOT NULL CHECK (price_usd > 0),
    source TEXT NOT NULL DEFAULT 'coingecko',
    extracted_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (coin, extracted_at)
);

CREATE INDEX IF NOT EXISTS idx_silver_coin_time
    ON crypto_prices_silver (coin, extracted_at DESC);

-- ============================================================
-- GOLD LAYER: текущая цена по каждой монете (снэпшот "сейчас")
-- Обновляется через UPSERT — одна строка на монету
-- ============================================================
CREATE TABLE IF NOT EXISTS crypto_latest_prices (
    coin TEXT PRIMARY KEY,
    price_usd NUMERIC(20, 8) NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- GOLD LAYER: агрегаты по дням (min/max/avg/volatility per coin per day)
-- PRIMARY KEY (summary_date, coin) — одна строка на (день, монета)
-- ============================================================
CREATE TABLE IF NOT EXISTS crypto_daily_summary (
    summary_date DATE NOT NULL,
    coin TEXT NOT NULL,
    min_price NUMERIC(20, 8) NOT NULL,
    max_price NUMERIC(20, 8) NOT NULL,
    avg_price NUMERIC(20, 8) NOT NULL,
    volatility NUMERIC NOT NULL DEFAULT 0,
    records_count INT NOT NULL,
    PRIMARY KEY (summary_date, coin)
);

-- ============================================================
-- GOLD LAYER: изменения цены (current vs 24h/7d/30d ago, в %)
-- Одна строка на монету — обновляется через UPSERT
-- ============================================================
-- Колонку snapshot_time не называем current_time, т.к. current_time — это
-- зарезервированное слово PostgreSQL (встроенная функция CURRENT_TIME).
CREATE TABLE IF NOT EXISTS crypto_price_changes (
    coin TEXT PRIMARY KEY,
    current_price NUMERIC(20, 8) NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    price_24h_ago NUMERIC(20, 8),
    price_7d_ago NUMERIC(20, 8),
    price_30d_ago NUMERIC(20, 8),
    change_24h_pct NUMERIC,
    change_7d_pct NUMERIC,
    change_30d_pct NUMERIC,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ANALYTICS: агрегированные метрики по монетам
-- ============================================================
CREATE TABLE IF NOT EXISTS crypto_metrics (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    avg_price NUMERIC(20, 8),
    min_price NUMERIC(20, 8),
    max_price NUMERIC(20, 8),
    records_count INT
);

-- ============================================================
-- ANALYTICS: временные ряды с MA и волатильностью
-- ============================================================
CREATE TABLE IF NOT EXISTS crypto_analytics (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    price_usd NUMERIC(20, 8),
    extracted_at TIMESTAMPTZ,
    daily_return NUMERIC,
    ma_7 NUMERIC,
    ma_30 NUMERIC,
    volatility_7 NUMERIC,
    volatility_30 NUMERIC
);
