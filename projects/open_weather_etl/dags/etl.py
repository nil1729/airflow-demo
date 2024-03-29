from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract import _extract
from transform import _transform
from load import _load
from airflow.providers.http.sensors.http import HttpSensor

with DAG(
    dag_id='openweather_etl',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['openweather'],
    start_date=datetime(year=2023, month=1, day=1)
) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='openweather_conn',
        method='GET',
        timeout=30,
        poke_interval=10,
        endpoint=''
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=_extract
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=_transform
    )

    load = PythonOperator(
        task_id='load',
        python_callable=_load
    )

    is_api_available >> extract >> transform >> load
