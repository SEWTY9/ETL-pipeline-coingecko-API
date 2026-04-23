from sqlalchemy import create_engine, text

import pandas as pd

import logging

logger = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:postgres@postgres:5432/crypto_db"


def calculate_metrics() -> None:
    """Расчёт агрегированных метрик по silver-слою"""

    engine = create_engine(DB_URL)

    logger.info("Calculating SQL metrics from silver layer...")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE crypto_metrics"))

        query = text("""
            INSERT INTO crypto_metrics (coin, avg_price, min_price, max_price, records_count)
            SELECT
                coin,
                AVG(price_usd) AS avg_price,
                MIN(price_usd) AS min_price,
                MAX(price_usd) AS max_price,
                COUNT(*) AS records_count
            FROM crypto_prices_silver
            GROUP BY coin
        """)

        conn.execute(query)

    logger.info("Metrics recalculated in crypto_metrics")


def pandas_analytics() -> None:
    """Аналитика с pandas: MA, daily return, volatility — из silver"""

    engine = create_engine(DB_URL)

    logger.info("Running pandas analytics on silver layer...")

    df = pd.read_sql(
        "SELECT coin, price_usd, extracted_at FROM crypto_prices_silver ORDER BY extracted_at",
        engine,
    )

    if df.empty:
        logger.warning("No data to process in pandas analytics")
        return

    df["price_usd"] = df["price_usd"].astype(float)
    df = df.sort_values(["coin", "extracted_at"])

    grouped = df.groupby("coin", group_keys=False)
    df["daily_return"] = grouped["price_usd"].pct_change()
    df["ma_7"] = grouped["price_usd"].transform(lambda s: s.rolling(7, min_periods=1).mean())
    df["ma_30"] = grouped["price_usd"].transform(lambda s: s.rolling(30, min_periods=1).mean())
    df["volatility_7"] = grouped["price_usd"].transform(
        lambda s: s.rolling(7, min_periods=1).std().fillna(0)
    )
    df["volatility_30"] = grouped["price_usd"].transform(
        lambda s: s.rolling(30, min_periods=1).std().fillna(0)
    )

    result = df[[
        "coin", "price_usd", "extracted_at",
        "daily_return", "ma_7", "ma_30",
        "volatility_7", "volatility_30",
    ]]

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE crypto_analytics"))

    result.to_sql("crypto_analytics", engine, if_exists="append", index=False)

    logger.info(f"Pandas analytics completed: {len(result)} rows stored")
