# Crypto ETL Pipeline

<div align="center">

**ETL-пайплайн для сбора и аналитики данных о криптовалютах в реальном времени**

![Python](https://img.shields.io/badge/Python-3.11-3776ab?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Analytics-150458?logo=pandas&logoColor=white)

</div>

---

## О проекте

Пайплайн каждые 12 часов забирает цены 15 криптовалют из публичного API **CoinGecko**, проводит их через 3 слоя обработки (**Bronze → Silver → Gold**) и формирует бизнес-витрины для аналитики. Оркестрация через **Apache Airflow**, хранение в **PostgreSQL**, всё запускается одной командой `docker-compose up -d`.

Проект построен на классическом для Data Engineering паттерне **Medallion Architecture** и демонстрирует работу с:
- оконными функциями SQL (`DISTINCT ON`, `LAG`), CTE, UPSERT (`ON CONFLICT DO UPDATE`)
- временными рядами в Pandas (moving averages, volatility, daily returns)
- оркестрацией задач через DAG с зависимостями
- контейнеризацией multi-service приложения

---

## Архитектура

```
                         CoinGecko API
                               │
                     ┌─────────▼─────────┐
                     │ check_database    │ ── health-check Postgres
                     └─────────┬─────────┘
                               │
                     ┌─────────▼─────────┐
                     │ extract_data      │ ── HTTP GET /simple/price
                     └──────┬──────┬─────┘
                            │      │
          ┌─────────────────▼──┐   │
          │ load_raw_data      │   │
          │ (BRONZE — JSONB)   │   │
          └────────────────────┘   │
                                   │
                       ┌───────────▼──────────┐
                       │ transform_data       │ ── JSON → rows
                       └───────────┬──────────┘
                                   │
                       ┌───────────▼──────────┐
                       │ load_silver_data     │ ── валидация + dedup
                       │ (SILVER)             │    + NUMERIC(20,8)
                       └───────────┬──────────┘
                                   │
         ┌──────────┬───────────┬──┴──────┬───────────────┐
         │          │           │         │               │
   ┌─────▼──┐ ┌─────▼────┐ ┌────▼───┐ ┌───▼────┐ ┌────────▼──────┐
   │latest_ │ │daily_    │ │price_  │ │sql_    │ │pandas_        │
   │prices  │ │summary   │ │changes │ │metrics │ │analytics      │
   └────────┘ └──────────┘ └────────┘ └────────┘ └───────────────┘
    GOLD       GOLD         GOLD      ANALYTICS   ANALYTICS
```

### Слои данных

| Слой | Роль | Пример |
|------|------|--------|
| **Bronze** | Сырые данные от API как есть, для реплея и аудита | JSONB-поле с полным ответом CoinGecko |
| **Silver** | Очищенные, провалидированные, типизированные строки | `crypto_prices_silver` — `NUMERIC(20,8)`, `CHECK (price > 0)`, `UNIQUE (coin, extracted_at)` |
| **Gold** | Бизнес-витрины, готовые для отчётов и дашбордов | `latest_prices`, `daily_summary`, `price_changes` |
| **Analytics** | Надстройка над silver: агрегаты и временные ряды | `crypto_metrics`, `crypto_analytics` |

---

## Витрины данных (Gold)

### `crypto_latest_prices`
Актуальная цена по каждой монете (одна строка на coin). Обновляется через UPSERT.

```sql
SELECT coin, price_usd, extracted_at
FROM crypto_latest_prices
ORDER BY price_usd DESC;
```

### `crypto_daily_summary`
Агрегаты по дням: min / max / avg цена, волатильность, количество записей. PK `(summary_date, coin)`.

```sql
SELECT summary_date, coin, min_price, max_price, avg_price, volatility
FROM crypto_daily_summary
WHERE coin = 'bitcoin'
ORDER BY summary_date DESC
LIMIT 30;
```

### `crypto_price_changes`
Текущая цена + изменение за **24h / 7d / 30d** в процентах. Построено на CTE с `DISTINCT ON`.

```sql
SELECT coin, current_price, change_24h_pct, change_7d_pct, change_30d_pct
FROM crypto_price_changes
ORDER BY change_24h_pct DESC NULLS LAST
LIMIT 10;
```

---

## Технологии

| Компонент | Версия | Назначение |
|-----------|--------|------------|
| Python | 3.11 | Runtime для ETL и Airflow |
| Apache Airflow | 2.9.1 | Оркестрация DAG, расписание, ретраи |
| PostgreSQL | 15 | Хранилище данных (все слои) |
| Pandas | latest | Аналитика временных рядов (MA, volatility) |
| SQLAlchemy | latest | Работа с БД из Python |
| Docker Compose | — | Оркестрация multi-service окружения |

---

## Быстрый старт

### Требования
- Docker + Docker Compose
- Свободные порты: `5432` (Postgres), `5050` (pgAdmin), `8080` (Airflow)
- 4+ ГБ свободной RAM (Airflow webserver + scheduler)

### Запуск

```bash
# Клонируем
git clone https://github.com/SEWTY9/ETL-pipeline-coingecko-API.git
cd ETL-pipeline-coingecko-API

# Поднимаем стек
docker-compose up -d

# При изменениях в db/init.sql нужно пересоздать volume
docker-compose down -v && docker-compose up -d
```

### Доступ к сервисам

| Сервис | URL | Логин / пароль |
|--------|-----|----------------|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| pgAdmin | http://localhost:5050 | `admin@admin.com` / `admin` |
| PostgreSQL | `localhost:5432` | `postgres` / `postgres` |

### Запуск пайплайна

1. Открой Airflow UI → найди DAG `crypto_etl_pipeline`
2. Переключи тоггл в **ON**
3. Нажми **▶ Trigger DAG** для ручного запуска, либо жди автозапуска (`0 */12 * * *`)
4. Проверь данные в pgAdmin или через `docker exec crypto_postgres psql -U postgres -d crypto_db`

---

## Структура проекта

```
my-first-ETL/
├── dags/
│   └── crypto_etl_dag.py        # DAG Airflow: граф задач
├── db/
│   └── init.sql                 # DDL всех 7 таблиц
├── etl/
│   ├── main.py                  # extract / transform / load_raw / load_silver
│   ├── gold.py                  # Gold-билдеры (latest/daily/changes)
│   ├── analytics.py             # SQL metrics + Pandas analytics
│   ├── Dockerfile               # Образ ETL-контейнера
│   └── requirements.txt         # pandas, requests, sqlalchemy, psycopg2-binary
├── docker-compose.yml           # Весь стек (postgres, airflow, pgadmin, etl)
├── .env                         # POSTGRES_USER/PASSWORD/DB
└── README.md
```

---

## SQL-паттерны в проекте

| Паттерн | Где используется | Что делает |
|---------|------------------|------------|
| `DISTINCT ON (col)` | `latest_prices`, `price_changes` | Оставляет одну строку на группу по `ORDER BY` |
| `ON CONFLICT DO UPDATE` | все gold-таблицы | UPSERT — идемпотентное обновление |
| `ON CONFLICT DO NOTHING` | silver-loader | Защита от дублей при Airflow-ретраях |
| CTE + `JOIN` | `price_changes` | Поиск "цены N времени назад" через цепочку CTE |
| `STDDEV()` + `COALESCE` | `daily_summary` | Волатильность с обработкой единичных значений |
| `extracted_at::date` | `daily_summary` | Каст `TIMESTAMPTZ` в `DATE` для группировки |

---

## Roadmap

### Готово
- [x] Medallion-архитектура: bronze → silver → gold
- [x] Валидация + дедуп в silver-слое
- [x] 3 gold-витрины с UPSERT
- [x] Retry-safe timestamps через Airflow context
- [x] Векторизация pandas-аналитики
- [x] Индексы на silver для ускорения gold-запросов

### В планах
- [ ] Тесты: `pytest` для `transform()` и gold-билдеров
- [ ] Таймауты и retry на HTTP-запросах к CoinGecko
- [ ] Вынос списка монет в конфиг
- [ ] Уведомления в Telegram при падении DAG
- [ ] Визуализация через Grafana/Metabase

---

## Автор

**Глеб Евграфов** ([SEWTY9](https://github.com/SEWTY9))

Студент 3 курса МТКП МГТУ им. Н. Э. Баумана. Учебный проект для изучения ETL-паттернов и Data Engineering.

---

<div align="center">

⭐ Если проект был полезен — не забудь звёздочку!

</div>
