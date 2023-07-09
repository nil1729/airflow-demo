from airflow import DAG, Dataset
from datetime import datetime
from airflow.decorators import task

my_file = Dataset(
    uri='/tmp/demo_file.txt',
    extra={'owner': 'nilanjan.deb'}
)

with DAG(
    dag_id='producer',
    schedule='@daily',
    start_date=datetime(year=2023, month=1, day=1),
    catchup=False
):
    @task(outlets=[my_file])
    def update_dataset():
        text = f'producer update [{datetime.now().timestamp()}]'
        with open(my_file.uri, 'a') as file:
            file.write(f'{text}\n')

    update_dataset()
