import requests
import json
import time
from sqlalchemy import create_engine, text
import logging
from typing import Dict, List, Any, Optional


from analytics import calculate_metrics
from analytics import pandas_analytics


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


def transform(raw_data: Optional[Dict[str, Any]] = None, **context) -> List[Dict[str, str]]:
    """Трансформация данных"""

    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data')

    rows = []

    logger.info("Transforming data...")

    for coin, values in raw_data.items():
        row = {
            "coin": coin,
            "price": values["usd"]
        }
        rows.append(row)

    logger.info(f"Transformed {len(rows)} records")
    return rows


def load_raw(raw_data: Optional[Dict[str, Any]] = None, **context) -> None:
    """Загрузка в бронзовый слой (raw)"""

    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data')

    engine = create_engine(DB_URL)

    logger.info("Loading raw data to database...")

    with engine.connect() as conn:
        query = text("""
            INSERT INTO raw_crypto_data (raw_json)
            VALUES (:data)
        """)
        conn.execute(query, {"data": json.dumps(raw_data)})

    logger.info("Raw data loaded successfully")


def load_gold(rows: Optional[List[Dict[str, Any]]] = None, **context) -> None:
    """Загрузка в голд слой (crypto_prices)"""

    ti = context['ti']
    rows = ti.xcom_pull(task_ids='transform_data')

    engine = create_engine(DB_URL)

    logger.info(f"Loading {len(rows)} records to gold layer...")

    with engine.connect() as conn:
        for row in rows:
            query = text("""
                INSERT INTO crypto_prices (coin, price_usd, loaded_at)
                VALUES (:coin, :price, NOW())
            """)
            conn.execute(query, row)

    logger.info(f"Loaded {len(rows)} records to crypto_prices")


def main() -> None:
    """Основная функция для запуска без Airflow"""

    wait_for_db()

    raw_data = extract()
    load_raw(raw_data=raw_data)

    transformed = transform(raw_data=raw_data)

    load_gold(rows=transformed)

    calculate_metrics()
    pandas_analytics()


if __name__ == "__main__":
    main()