from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def _t1():
    return 42  # key: return_value, value: 42


def _t2(ti):
    ti.xcom_push(key='key', value='value')  # key: key, value: value


def _t3(ti):
    t1_value = ti.xcom_pull(task_ids='t1', key='return_value')
    t2_value = ti.xcom_pull(task_ids='t2', key='key')
    print(f'''
        t1: {t1_value}
        t2: {t2_value}
    ''')


with DAG(
    dag_id='xcom_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=1, day=1),
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    t3 = PythonOperator(
        task_id='t3',
        python_callable=_t3
    )

    t1 >> t2 >> t3
