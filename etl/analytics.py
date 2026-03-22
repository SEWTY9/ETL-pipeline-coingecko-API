from sqlalchemy import create_engine, text

import pandas as pd

import logging
from typing import Any

logger = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:postgres@postgres:5432/crypto_db"


def calculate_metrics() -> None:
    """Расчет агрегированных метрик"""

    engine = create_engine(DB_URL)

    logger.info("Calculating SQL metrics...")

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE crypto_metrics"))

        query = text("""
            INSERT INTO crypto_metrics (coin, avg_price, min_price, max_price, records_count)
            SELECT
                coin,
                AVG(price_usd) AS avg_price,
                MIN(price_usd) AS min_price,
                MAX(price_usd) AS max_price,
                COUNT(*) AS records_count
            FROM crypto_prices
            GROUP BY coin
        """)

        conn.execute(query)
    
    logger.info("Metrics calculated and stored in crypto_metrics")


def pandas_analytics() -> None:
    """Аналитика с pandas"""

    engine = create_engine(DB_URL)

    logger.info("Running pandas analytics...")

    df = pd.read_sql("SELECT * FROM crypto_prices ORDER BY loaded_at", engine)

    if df.empty:
        logger.warning("No data to process in pandas analytics")
        print("No data to process")

    result_rows = []

    for coin, group in df.groupby("coin"):
        group = group.sort_values("loaded_at").copy()
        group["daily_return"] = group["price_usd"].pct_change()
        group["ma_7"] = group["price_usd"].rolling(7, min_periods=1).mean()
        group["ma_30"] = group["price_usd"].rolling(30, min_periods=1).mean()
        group["volatility_7"] = group["price_usd"].rolling(7, min_periods=1).std().fillna(0)
        group["volatility_30"] = group["price_usd"].rolling(30, min_periods=1).std().fillna(0)

        for _, row in group.iterrows():
            result_rows.append({
                "coin": coin,
                "price_usd": row["price_usd"],
                "loaded_at": row["loaded_at"],
                "daily_return": row["daily_return"],
                "ma_7": row["ma_7"],
                "ma_30": row["ma_30"],
                "volatility_7": row["volatility_7"],
                "volatility_30": row["volatility_30"],
            })

    result_df = pd.DataFrame(result_rows)
    result_df.to_sql("crypto_analytics", engine, if_exists="append", index=False)

    logger.info(f"Pandas analytics completed: {len(result_rows)} rows stored")
