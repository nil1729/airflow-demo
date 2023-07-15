from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable


def _load(ti):
    file_name = ti.xcom_pull(task_ids='transform', key='return_value')
    gcs_hook = GCSHook(gcp_conn_id='gcs_conn')
    with open(f'/tmp/{file_name}', 'rb') as file:
        data = file.read()
    gcs_hook.upload(
        bucket_name=Variable.get('gcs_bucket'),
        object_name=f'openweather/{file_name}',
        data=data,
    )
