from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='parallel_dag',
    start_date=datetime(year=2023, month=1, day=1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    extract_a = BashOperator(
        task_id='extract_a',
        bash_command='sleep 5'
    )

    extract_b = BashOperator(
        task_id='extract_b',
        bash_command='sleep 7'
    )

    load_a = BashOperator(
        task_id='load_a',
        bash_command='sleep 2'
    )

    load_b = BashOperator(
        task_id='load_b',
        bash_command='sleep 3'
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='sleep 10'
    )

    extract_a >> load_a >> transform
    extract_b >> load_b >> transform
