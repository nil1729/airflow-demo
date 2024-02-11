from airflow.models import Variable
import datetime


def get_dag_id(event_name):
    return 'day_' + event_name + '_summaries'


def get_dag_details(details):
    return {
        'dag_id': get_dag_id(details['event_name']),
        'depends_on_past': False,
        'email': Variable.get("alert_email"),
        'email_on_retry': False,
        'email_on_failure': True,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=15),
        'sla': datetime.timedelta(minutes=180),
        'max_active_runs': 1,
        'tags': ['FREQUENCY:DAILY', 'LEVEL:EXCEL_DUMP'] + details['dag_tags']
    }
