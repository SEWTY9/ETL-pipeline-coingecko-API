import requests
import json
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
import logging
from typing import Dict, List, Any, Optional


from analytics import calculate_metrics, pandas_analytics
from gold import build_latest_prices, build_daily_summary, build_price_changes


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DB_URL = "postgresql://postgres:postgres@postgres:5432/crypto_db"


def wait_for_db() -> bool:
    """Проверка доступности базы данных"""

    engine = create_engine(DB_URL)

    for attempt in range(10):
        try:
            with engine.connect():
                logger.info("Database is ready!")
                return True
        except Exception as e:
            logger.warning(f"Database not ready yet (attempt {attempt + 1}/10): {e}")
            time.sleep(5)

    raise Exception("Database is not ready after multiple attempts.")


def extract() -> Dict[str, Any]:
    """Извлечение данных из CoinGecko API"""

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,ripple,cardano,solana,polkadot,dogecoin,avalanche-2,chainlink,litecoin,uniswap,algorand,fantom,polygon,stellar",
        "vs_currencies": "usd"
    }

    try:
        logger.info("Extracting data from CoinGecko API...")
        response = requests.get(url, params=params)
        data = response.json()

        logger.info(f"Extracted data for {len(data)} cryptocurrencies")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to extract data: {e}")
        raise


def transform(raw_data: Optional[Dict[str, Any]] = None, **context) -> List[Dict[str, Any]]:
    """Трансформация данных: JSON → список плоских строк"""

    ti = context.get('ti')
    if ti is not None:
        raw_data = ti.xcom_pull(task_ids='extract_data')

    rows = []

    logger.info("Transforming data...")

    for coin, values in raw_data.items():
        row = {
            "coin": coin,
            "price": values.get("usd"),
        }
        rows.append(row)

    logger.info(f"Transformed {len(rows)} records")
    return rows


def load_raw(raw_data: Optional[Dict[str, Any]] = None, **context) -> None:
    """Загрузка в bronze-слой (raw) — JSONB как есть"""

    ti = context.get('ti')
    if ti is not None:
        raw_data = ti.xcom_pull(task_ids='extract_data')

    engine = create_engine(DB_URL)

    logger.info("Loading raw data to database...")

    with engine.begin() as conn:
        query = text("""
            INSERT INTO raw_crypto_data (raw_json)
            VALUES (:data)
        """)
        conn.execute(query, {"data": json.dumps(raw_data)})

    logger.info("Raw data loaded successfully")


def load_silver(
    rows: Optional[List[Dict[str, Any]]] = None,
    extracted_at: Optional[datetime] = None,
    **context,
) -> None:
    """
    Загрузка в silver-слой: валидация + дедуп + нормализация типов.

    - Отбрасывает строки где coin пустой или price_usd <= 0 / None
    - Использует extracted_at из Airflow-контекста (retry-safe)
    - ON CONFLICT DO NOTHING защищает от дублей по UNIQUE(coin, extracted_at)
    """

    ti = context.get('ti')
    if ti is not None:
        rows = ti.xcom_pull(task_ids='transform_data')
        extracted_at = context.get('logical_date') or context.get('data_interval_start')

    if extracted_at is None:
        extracted_at = datetime.now(timezone.utc)

    if not rows:
        logger.warning("No rows received in load_silver, skipping")
        return

    valid_rows = []
    rejected = 0
    for row in rows:
        coin = row.get("coin")
        price = row.get("price")
        if not coin or price is None or price <= 0:
            rejected += 1
            continue
        valid_rows.append({
            "coin": coin,
            "price_usd": price,
            "extracted_at": extracted_at,
        })

    logger.info(
        f"Silver validation: {len(valid_rows)} valid, {rejected} rejected"
    )

    if not valid_rows:
        logger.warning("No valid rows to load into silver")
        return

    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        query = text("""
            INSERT INTO crypto_prices_silver (coin, price_usd, extracted_at)
            VALUES (:coin, :price_usd, :extracted_at)
            ON CONFLICT (coin, extracted_at) DO NOTHING
        """)
        conn.execute(query, valid_rows)

    logger.info(f"Loaded {len(valid_rows)} rows into crypto_prices_silver")


def main() -> None:
    """Запуск пайплайна без Airflow (для локальной отладки)"""

    wait_for_db()

    raw_data = extract()
    load_raw(raw_data=raw_data)

    transformed = transform(raw_data=raw_data)

    extracted_at = datetime.now(timezone.utc)
    load_silver(rows=transformed, extracted_at=extracted_at)

    build_latest_prices()
    build_daily_summary()
    build_price_changes()

    calculate_metrics()
    pandas_analytics()


if __name__ == "__main__":
    main()
