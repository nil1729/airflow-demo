import pandas as pd


def _transform(ti):
    extract_value = ti.xcom_pull(task_ids='extract', key='return_value')
    transformed_data = pd.json_normalize({
        'latitude': extract_value['coord']['lat'],
        'longitude': extract_value['coord']['lon'],
        'temperature': extract_value['main']['temp'],
        'humidity': extract_value['main']['humidity'],
        'pressure': extract_value['main']['pressure'],
        'timestamp': extract_value['dt']
    })
    file_name = f"{extract_value['dt']}.csv"
    transformed_data.to_csv(f'/tmp/{file_name}', header=True, index=False)
    return file_name
