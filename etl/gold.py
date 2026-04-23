"""
Gold layer builders — витрины для бизнес-аналитики.

Все функции читают из silver (crypto_prices_silver) и записывают
в собственные gold-таблицы. Предполагается запуск после load_silver.
"""

from sqlalchemy import create_engine, text

import logging

logger = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:postgres@postgres:5432/crypto_db"


def build_latest_prices() -> None:
    """
    Gold #1: последняя цена по каждой монете.

    Использует DISTINCT ON (coin) — PostgreSQL-специфичный оператор,
    который для каждой группы оставляет первую строку по ORDER BY.

    Пишет через UPSERT (ON CONFLICT DO UPDATE), поэтому таблица всегда
    содержит одну актуальную строку на монету.
    """

    engine = create_engine(DB_URL)

    logger.info("Building gold: crypto_latest_prices...")

    with engine.begin() as conn:
        query = text("""
            INSERT INTO crypto_latest_prices (coin, price_usd, extracted_at, updated_at)
            SELECT DISTINCT ON (coin)
                coin,
                price_usd,
                extracted_at,
                NOW() AS updated_at
            FROM crypto_prices_silver
            ORDER BY coin, extracted_at DESC
            ON CONFLICT (coin) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                extracted_at = EXCLUDED.extracted_at,
                updated_at = NOW()
        """)
        conn.execute(query)

    logger.info("Gold crypto_latest_prices built successfully")


def build_daily_summary() -> None:
    """
    Gold #2: агрегаты по дням.

    Для каждой пары (день, монета) считает min/max/avg цену, волатильность
    (стандартное отклонение) и количество записей.

    STDDEV из SQL == np.std(ddof=1). Если в группе одна запись — STDDEV
    вернёт NULL, поэтому оборачиваем в COALESCE.
    """

    engine = create_engine(DB_URL)

    logger.info("Building gold: crypto_daily_summary...")

    with engine.begin() as conn:
        query = text("""
            INSERT INTO crypto_daily_summary
                (summary_date, coin, min_price, max_price, avg_price, volatility, records_count)
            SELECT
                extracted_at::date AS summary_date,
                coin,
                MIN(price_usd) AS min_price,
                MAX(price_usd) AS max_price,
                AVG(price_usd) AS avg_price,
                COALESCE(STDDEV(price_usd), 0) AS volatility,
                COUNT(*) AS records_count
            FROM crypto_prices_silver
            GROUP BY extracted_at::date, coin
            ON CONFLICT (summary_date, coin) DO UPDATE SET
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                avg_price = EXCLUDED.avg_price,
                volatility = EXCLUDED.volatility,
                records_count = EXCLUDED.records_count
        """)
        conn.execute(query)

    logger.info("Gold crypto_daily_summary built successfully")


def build_price_changes() -> None:
    """
    Gold #3: изменение цены за 24h / 7d / 30d.

    Для каждой монеты ищем:
      - current_price = последняя запись в silver
      - price_Xh_ago = последняя запись, у которой extracted_at <= now - X

    "Последняя запись <= момента в прошлом" == ближайшая точка ДО целевого
    момента. Если данных за период нет (проект только запустили), возвращаем
    NULL — процент изменения тоже будет NULL.

    Используем CTE + DISTINCT ON — PostgreSQL-идиома, которая для каждой
    группы оставляет первую строку по ORDER BY. Альтернатива — ROW_NUMBER()
    OVER (PARTITION BY coin ORDER BY extracted_at DESC), но DISTINCT ON короче.
    """

    engine = create_engine(DB_URL)

    logger.info("Building gold: crypto_price_changes...")

    with engine.begin() as conn:
        query = text("""
            INSERT INTO crypto_price_changes (
                coin, current_price, snapshot_time,
                price_24h_ago, price_7d_ago, price_30d_ago,
                change_24h_pct, change_7d_pct, change_30d_pct,
                updated_at
            )
            WITH latest AS (
                SELECT DISTINCT ON (coin)
                    coin,
                    price_usd AS current_price,
                    extracted_at AS snapshot_time
                FROM crypto_prices_silver
                ORDER BY coin, extracted_at DESC
            ),
            price_24h AS (
                SELECT DISTINCT ON (s.coin)
                    s.coin,
                    s.price_usd AS price_24h_ago
                FROM crypto_prices_silver s
                JOIN latest l ON l.coin = s.coin
                WHERE s.extracted_at <= l.snapshot_time - INTERVAL '24 hours'
                ORDER BY s.coin, s.extracted_at DESC
            ),
            price_7d AS (
                SELECT DISTINCT ON (s.coin)
                    s.coin,
                    s.price_usd AS price_7d_ago
                FROM crypto_prices_silver s
                JOIN latest l ON l.coin = s.coin
                WHERE s.extracted_at <= l.snapshot_time - INTERVAL '7 days'
                ORDER BY s.coin, s.extracted_at DESC
            ),
            price_30d AS (
                SELECT DISTINCT ON (s.coin)
                    s.coin,
                    s.price_usd AS price_30d_ago
                FROM crypto_prices_silver s
                JOIN latest l ON l.coin = s.coin
                WHERE s.extracted_at <= l.snapshot_time - INTERVAL '30 days'
                ORDER BY s.coin, s.extracted_at DESC
            )
            SELECT
                l.coin,
                l.current_price,
                l.snapshot_time,
                p24.price_24h_ago,
                p7.price_7d_ago,
                p30.price_30d_ago,
                CASE WHEN p24.price_24h_ago > 0
                     THEN (l.current_price - p24.price_24h_ago) / p24.price_24h_ago * 100
                     ELSE NULL END,
                CASE WHEN p7.price_7d_ago > 0
                     THEN (l.current_price - p7.price_7d_ago) / p7.price_7d_ago * 100
                     ELSE NULL END,
                CASE WHEN p30.price_30d_ago > 0
                     THEN (l.current_price - p30.price_30d_ago) / p30.price_30d_ago * 100
                     ELSE NULL END,
                NOW()
            FROM latest l
            LEFT JOIN price_24h p24 USING (coin)
            LEFT JOIN price_7d p7 USING (coin)
            LEFT JOIN price_30d p30 USING (coin)
            ON CONFLICT (coin) DO UPDATE SET
                current_price = EXCLUDED.current_price,
                snapshot_time = EXCLUDED.snapshot_time,
                price_24h_ago = EXCLUDED.price_24h_ago,
                price_7d_ago = EXCLUDED.price_7d_ago,
                price_30d_ago = EXCLUDED.price_30d_ago,
                change_24h_pct = EXCLUDED.change_24h_pct,
                change_7d_pct = EXCLUDED.change_7d_pct,
                change_30d_pct = EXCLUDED.change_30d_pct,
                updated_at = EXCLUDED.updated_at
        """)
        conn.execute(query)

    logger.info("Gold crypto_price_changes built successfully")
