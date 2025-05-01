import json
import requests
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client  

from airflow import DAG
from airflow.operators.python import PythonOperator


# Константы
CH_CLIENT = Client(
    host='158.160.116.58',
    user='student',
    password='dfqh89fhq8',
    database='sandbox'
)

DATE_COL = 'date'
CURRENCY_SOURCE_COL = 'currency_source'
CURRENCY_COL = 'currency'
VALUE_COL = 'value'


def get_api_exchangerate_data(
    day: str,
    currency_source: str,
    access_key: str,
    file_path: str,
    api_url='https://api.exchangerate.host/timeframe',
):
    """
    Метод получения данных с api.exchangerate.host за день.
    И сохраняет json response ответ в локальный файл.
    
    :param day: За какой день выбрать данные, формат YYYY-MM-DD.
    :param currency_source: Валюта для которой забираются данные.
    :param access_key: Ключ доступа для api.
    :param file_path: Путь до файла.
    :param api_url: Api url.
    """
    params = dict(
        access_key=access_key,
        source=currency_source,
        start_date=day,
        end_date=day,
    )

    req = requests.get(api_url, params=params)
    
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(req.json(), file, ensure_ascii=False, sort_keys=True, indent=4)


def transform_exchangerate_data(
    src_file_path: str,
    dst_file_path: str,
):
    """Преобразование json response в csv таблицу.

    :param src_file_path: Путь до json файла.
    :param dst_file_path: Путь до csv таблицы.
    """
    SRC_DST = 'src_dst'

    with open(src_file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    source = json_data['source']
    day = json_data['start_date']

    data = pd.DataFrame.from_dict(json_data['quotes'][day], orient='index').reset_index()
    data.columns = [SRC_DST, VALUE_COL]
    
    data[DATE_COL] = day
    data[CURRENCY_SOURCE_COL] = source
    data[CURRENCY_COL] = data[SRC_DST].str.slice(len(source))
    data.drop(SRC_DST, axis=1, inplace=True)

    data.to_csv(dst_file_path, encoding='utf-8', index=False)


def upload_to_clickhouse(csv_file_path, table_name, client):
    """
    Считывание CSV файл, создание таблицу в базе данных ClickHouse и добавление данных в неё.
    
    :param csv_file_path: Путь до файла с данными.
    :param table_name: Имя таблицы в БД.
    :param client: Клиент подключения к ClickHouse.
    """

    data: pd.DataFrame = pd.read_csv(csv_file_path)

    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (date String, currency_source String, currency String, value Float32) ENGINE Log')

    client.execute(f'INSERT INTO {table_name} VALUES', data.to_dict('records'))


table_name = '43342120_hm_31'

day_check = '2023-01-01'
currency_source = 'USD'
secret_key = '422bab0c9e08a5476912061877017b8d'

json_file_path = 'response.json'
data_file_path = 'table_data.csv'


with DAG(
    dag_id='43342120_hm_31',
    schedule_interval=None,
    start_date=datetime(2024,1,1),
) as dag:
    
    extract_task = PythonOperator(
        task_id='get_api_exchangerate_data',
        python_callable=get_api_exchangerate_data,
        op_kwargs={
            'day': day_check,
            'currency_source': currency_source,
            'access_key': secret_key,
            'file_path': json_file_path,
        },
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_exchangerate_data,
        op_kwargs={
            'src_file_path': json_file_path,
            'dst_file_path': data_file_path,
        },
    )

    upload_task = PythonOperator(
        task_id='upload_task',
        python_callable=upload_to_clickhouse,
        op_kwargs={
            'csv_file_path': data_file_path,
            'table_name': table_name, 
            'client': CH_CLIENT,
        },
    )

    extract_task >> transform_task  >> upload_task
