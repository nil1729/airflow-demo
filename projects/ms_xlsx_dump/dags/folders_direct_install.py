from airflow import DAG
from job_template import create_excel_dump_job
from job_config import get_dag_id
import datetime

schema_name = '{{SCHEMA_NAME}}'
event_name = '{{EVENT_NAME}}'
onedrive_file_id = '{{FILE_ID}}'

dag_info = {
    'owner': 'nil1729',
    'start_date': datetime.datetime(year=2023, month=9, day=25),
    'dag_tags': ['TEAM:PRODUCT'],
    'drive_file_id': onedrive_file_id,
    'schema': schema_name,
    'event_name': event_name,
    'ddl_col_type_mapping': [
        {'col': '{{COLUMN_NAME}}', 'data_type': '{{DATA_TYPE}}'},
        # .....
        # {'col': '{{COLUMN_NAME}}', 'data_type': '{{DATA_TYPE}}'},
    ],
    # ddl column order and dest column order should exact same
    'dest_column_mapping': [
        {'src': '{{XLSX_COLUMN_NAME}}', 'dest': '{{PG_TABLE_COLUMN_NAME}}'},
        # ......
        # {'src': '{{XLSX_COLUMN_NAME}}', 'dest': '{{PG_TABLE_COLUMN_NAME}}'},
    ]
}

globals()[get_dag_id(dag_info['event_name'])] = create_excel_dump_job(dag_info)
