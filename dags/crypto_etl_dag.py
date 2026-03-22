from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys

from etl.main import extract, transform, load_raw, load_gold, wait_for_db
from etl.analytics import calculate_metrics, pandas_analytics

sys.path.append('/opt/airflow/etl')


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}


with DAG(
    dag_id='crypto_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for cryptocurrency data',
    schedule_interval='0 */12 * * *',
    catchup=False,
    tags=['crypto', 'etl', 'analytics'],
    max_active_runs=1,
) as dag:

    check_db = PythonOperator(
        task_id='check_database',
        python_callable=wait_for_db
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    load_raw_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        provide_context=True
    )

    load_gold_task = PythonOperator(
        task_id='load_gold_data',
        python_callable=load_gold,
        provide_context=True
    )

    metrics_task = PythonOperator(
        task_id='calculate_sql_metrics',
        python_callable=calculate_metrics
    )

    pandas_task = PythonOperator(
        task_id='pandas_analytics',
        python_callable=pandas_analytics
    )

    check_db >> extract_task

    extract_task >> [load_raw_task, transform_task]

    transform_task >> load_gold_task

    load_gold_task >> [metrics_task, pandas_task]


