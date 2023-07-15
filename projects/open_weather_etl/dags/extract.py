import requests
from airflow.models import Variable


def _extract():
    api_host = Variable.get('openweather_api_host')
    api_key = Variable.get('openweather_api_key')

    city = 'Delhi'
    url = f'{api_host}?q={city}&appid={api_key}'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print('Error:', response.status_code)
        exit(1)
