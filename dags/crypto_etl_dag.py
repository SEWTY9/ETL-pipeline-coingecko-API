from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys

from etl.main import extract, transform, load_raw, load_silver, wait_for_db
from etl.analytics import calculate_metrics, pandas_analytics
from etl.gold import build_latest_prices, build_daily_summary, build_price_changes

sys.path.append('/opt/airflow/etl')


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    dag_id='crypto_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for cryptocurrency data (bronze → silver → analytics)',
    schedule_interval='0 */12 * * *',
    catchup=False,
    tags=['crypto', 'etl', 'analytics'],
    max_active_runs=1,
) as dag:

    check_db = PythonOperator(
        task_id='check_database',
        python_callable=wait_for_db,
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
    )

    load_raw_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )

    load_silver_task = PythonOperator(
        task_id='load_silver_data',
        python_callable=load_silver,
    )

    latest_prices_task = PythonOperator(
        task_id='gold_latest_prices',
        python_callable=build_latest_prices,
    )

    daily_summary_task = PythonOperator(
        task_id='gold_daily_summary',
        python_callable=build_daily_summary,
    )

    price_changes_task = PythonOperator(
        task_id='gold_price_changes',
        python_callable=build_price_changes,
    )

    metrics_task = PythonOperator(
        task_id='calculate_sql_metrics',
        python_callable=calculate_metrics,
    )

    pandas_task = PythonOperator(
        task_id='pandas_analytics',
        python_callable=pandas_analytics,
    )

    check_db >> extract_task

    extract_task >> [load_raw_task, transform_task]

    transform_task >> load_silver_task

    load_silver_task >> [
        latest_prices_task,
        daily_summary_task,
        price_changes_task,
        metrics_task,
        pandas_task,
    ]
