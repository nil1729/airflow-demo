from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup


def download_tasks():
    with TaskGroup(group_id='download_tasks', tooltip='Download Tasks') as group:
        download_a = BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )

        download_b = BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )

        download_c = BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )

        return group


def transform_tasks():
    with TaskGroup(group_id='transform_tasks', tooltip='Transform Tasks') as group:
        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 10'
        )

        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )

        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )

        return group


with DAG(
    dag_id='group_dag_by_task_group',
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=1, day=1),
    catchup=False
) as dag:
    downloads = download_tasks()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms = transform_tasks()

    downloads >> check_files >> transforms

