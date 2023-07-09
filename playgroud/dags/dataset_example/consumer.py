from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

my_file = Dataset(
    uri='/tmp/demo_file.txt'
)

with DAG(
    dag_id='consumer',
    schedule=[my_file],
    start_date=datetime(year=2023, month=1, day=1),
    catchup=False
):
    @task
    def read_dataset():
        with open(my_file.uri, 'r') as file:
            for line in file:
                print(line)

    read_dataset()
