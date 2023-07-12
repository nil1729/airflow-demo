from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.subdag import SubDagOperator


def subdag_downloads(parent_dag_id, child_dag_id, args):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        schedule_interval=args['schedule_interval'],
        start_date=args['start_date'],
        catchup=args['catchup']
    ) as subdag:
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

        return subdag


with DAG(
        dag_id='group_dag_by_subdag',
        schedule_interval='@daily',
        start_date=datetime(year=2023, month=1, day=1),
        catchup=False
) as dag:
    args = {
        'schedule_interval': dag.schedule_interval,
        'start_date': dag.start_date,
        'catchup': dag.catchup
    }

    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(dag.dag_id, 'downloads', args)
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    process_a = BashOperator(
        task_id='process_a',
        bash_command='sleep 10'
    )

    process_b = BashOperator(
        task_id='process_b',
        bash_command='sleep 10'
    )

    process_c = BashOperator(
        task_id='process_c',
        bash_command='sleep 10'
    )

    downloads >> check_files >> [process_a, process_b, process_c]
