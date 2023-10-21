from airflow import DAG
import requests
import logging
from pandas import read_excel
import numpy as np
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.db import provide_session
from airflow.models import XCom
from job_config import get_dag_details, get_dag_id
import os

logger = logging.getLogger()
tmp_dir_path = '{{TMP_DIR_PATH}}'
postgres_conn_id = '{{POSTGRES_CONN_ID}}'


class XlsxDumpHelper:
    def __init__(self, file_id, schema, event_name, ddl_col_type_mapping=None, dest_column_mapping=None):
        if dest_column_mapping is None or ddl_col_type_mapping is None:
            raise AirflowException('dest column mapping or ddl column data type not provided')
        self.file_id = file_id
        self.dest_table_name = '{}.{}'.format(schema, get_dag_id(event_name))
        self.dest_column_mapping = dest_column_mapping
        self.ddl_col_type_mapping = ddl_col_type_mapping

    def create_table_ddl(self):
        ddl_stmt = "CREATE TABLE IF NOT EXISTS {} (".format(self.dest_table_name)
        for col_type_map in self.ddl_col_type_mapping:
            ddl_stmt += '{} {},'.format(col_type_map['col'], col_type_map['data_type'])
        ddl_stmt += "process_date DATE);"
        return ddl_stmt

    def download_file(self, ti, **ctx):
        process_date = ctx.get('ds')
        file_name = '{}_{}.xlsx'.format(self.dest_table_name, process_date)
        file_path = '{}/{}'.format(tmp_dir_path, file_name)
        access_token = ti.xcom_pull(task_ids='get_access_token', key='access_token')
        _request_headers = {
            'Authorization': 'Bearer {}'.format(access_token)
        }
        file_url = 'https://graph.microsoft.com/v1.0/me/drive/items/{}/content'.format(self.file_id)
        response = requests.get(file_url, headers=_request_headers)
        if response.status_code == 200:
            with open(file_path, "wb") as file:
                file.write(response.content)
            ti.xcom_push(key='raw_file', value=file_path)
        else:
            logger.error(response.json())
            raise AirflowException("failed to download file")

    def get_existing_data(self, **ctx):
        process_date = ctx.get('ds')
        request = """
            SELECT count(*) AS total_count FROM {} WHERE process_date=DATE('{}')
            """.format(self.dest_table_name, process_date)
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        response = pg_hook.get_records(request)
        if response[0][0] == 0:
            return 'process_data'
        else:
            return 'clean_up'

    def process_data(self, ti, **ctx):
        process_date = ctx.get('ds')
        xlsx_file = ti.xcom_pull(task_ids='download_file', key='raw_file')
        processed_file_name = 'processed_{}_{}.csv'.format(self.dest_table_name, process_date)
        processed_file_path = '{}/{}'.format(tmp_dir_path, processed_file_name)
        df = read_excel(xlsx_file)
        row_count = len(df)
        new_column_values = np.full(row_count, process_date)
        df['process_date'] = new_column_values
        for column_map in self.dest_column_mapping:
            df.rename(columns={column_map['src']: column_map['dest']}, inplace=True)
        df.to_csv(processed_file_path, header=False, index=False)
        ti.xcom_push(key='processed_file', value=processed_file_path)

    def store_data(self, ti):
        processed_file = ti.xcom_pull(task_ids='process_data', key='processed_file')
        pg_hook = PostgresHook(postgres_conn_id='postgres_summary')
        pg_hook.copy_expert(
            sql="COPY {} FROM stdin WITH DELIMITER as ','".format(self.dest_table_name),
            filename=processed_file
        )

    # delete 7 days old data
    def purge_data(self, **ctx):
        process_date = ctx.get('ds')
        request = """
                    DELETE FROM {} WHERE process_date<DATE('{}') - INTERVAL '7' DAY 
                    RETURNING process_date
                    """.format(self.dest_table_name, process_date)
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        response = pg_hook.get_records(request)
        logger.info(response)


@provide_session
def _clean_xcom(session=None, **ctx):
    dag = ctx["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


def _get_access_token(ti):
    _request_body = {
        'client_id': Variable.get('{{MS_CLIENT_ID_VAR}}'),
        'client_secret': BaseHook.get_connection(conn_id='{{MS_CLIENT_SECRET}}').get_password(),
        'scope': 'Files.Read.All',
        # reason: localhost:9999 is set as redirect uri on Azure AD
        'redirect_uri': 'http://localhost:9999',
        'grant_type': 'refresh_token',
        'refresh_token': BaseHook.get_connection(conn_id='{{ACCOUNT_REFRESH_TOKEN}}').get_password(),
    }
    ms_auth_api = Variable.get('ms_auth_url')
    response = requests.post(ms_auth_api, data=_request_body)
    if response.status_code == 200:
        logger.info("successfully got the access token")
        data = response.json()
        ti.xcom_push(key='access_token', value=data['access_token'])
        BaseHook.get_connection(conn_id='{{ACCOUNT_REFRESH_TOKEN}}').set_password(data['refresh_token'])
    else:
        logger.error(response.json())
        raise AirflowException("failed to get the access token")


def _clean_up(ti):
    xlsx_file = ti.xcom_pull(task_ids='download_file', key='raw_file')
    processed_file = ti.xcom_pull(task_ids='process_data', key='processed_file')
    try:
        xlsx_file is not None and os.remove(xlsx_file)
        processed_file is not None and os.remove(processed_file)
    except FileNotFoundError or OSError or TypeError:
        logger.warning("task completed")


def create_dag(detail):
    default_args = {
        'owner': detail['owner'],
        'depends_on_past': detail['depends_on_past'],
        'start_date': detail['start_date'],
        'email': Variable.get("email"),
        'email_on_failure': detail['email_on_failure'],
        'email_on_retry': detail['email_on_retry'],
        'retries': detail['retries'],
        'retry_delay': detail['retry_delay'],
    }

    dag = DAG(dag_id=detail['dag_id'],
              default_args=default_args,
              max_active_runs=detail['max_active_runs'],
              tags=detail['tags'],
              # every day at 10:30 AM IST
              schedule_interval='0 5 * * *'
              )

    dump_helper = XlsxDumpHelper(
        file_id=detail['onedrive_file_id'],
        schema=detail['schema'],
        event_name=detail['event_name'],
        ddl_col_type_mapping=detail['ddl_col_type_mapping'],
        dest_column_mapping=detail['dest_column_mapping']
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_summary',
        sql=dump_helper.create_table_ddl(),
        dag=dag
    )

    get_access_token = PythonOperator(
        task_id='get_access_token',
        python_callable=_get_access_token,
        dag=dag
    )

    download_file = PythonOperator(
        task_id='download_file',
        python_callable=dump_helper.download_file,
        dag=dag
    )

    get_existing_data = BranchPythonOperator(
        task_id='get_existing_data',
        python_callable=dump_helper.get_existing_data,
        dag=dag
    )

    process_data = PythonOperator(
        task_id='process_data',
        provide_context=True,
        python_callable=dump_helper.process_data,
        dag=dag
    )

    store_data = PythonOperator(
        task_id='store_data',
        python_callable=dump_helper.store_data,
        dag=dag
    )

    purge_data = PythonOperator(
        task_id='purge_data',
        python_callable=dump_helper.purge_data,
        dag=dag
    )

    clean_up = PythonOperator(
        task_id='clean_up',
        python_callable=_clean_up,
        trigger_rule='none_failed_or_skipped',
        dag=dag
    )

    delete_xcom = PythonOperator(
        task_id="delete_xcom",
        python_callable=_clean_xcom,
        dag=dag
    )

    create_table >> get_access_token >> download_file >> get_existing_data >> process_data >> store_data >> purge_data >> clean_up >> delete_xcom
    get_existing_data >> clean_up >> delete_xcom

    return dag


def merge(a, b):
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key])
            else:
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a


def create_excel_dump_job(details):
    dag_config = merge(get_dag_details(details), details)
    return create_dag(dag_config)
