from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException # Нужно для вызова исключений в Airflow

import pandas as pd
import requests


FILE_DIFFERENCE = 'file_difference'
FILES_FULL = 'files_full'

CH_CLIENT = Client(
    host=BaseHook.get_connection("clickhouse_default").host,
    user=BaseHook.get_connection("clickhouse_default").login,
    password=BaseHook.get_connection("clickhouse_default").password,
    database=BaseHook.get_connection("clickhouse_default").schema,
)


def fetch_data_to_xcom(api_url, **kwargs):
        
        # Получаем все файлы из Xcom на вчера, если такого ключа нет, то будет возвращен None
        task_instance = kwargs['task_instance']

        # Данный параметр включает доступ к Xcom для всех предыдущих запусков
        # из одинаковых ключей будет выбран последний ключ по времени
        files_on_server = task_instance.xcom_pull(
            key='files_full', 
            include_prior_dates=True,
        ) or []
        
        # Получаем данные с сервера
        response = requests.get(api_url + kwargs['ds'])  
        
        if response.status_code == 200:
            # Парсинг JSON ответа
            all_files_to_upload = response.json()['files']
            
            # Находим разницу в 2 массивах
            # НАЙДИТЕ РАЗНИЦУ МАССИВОВ ДАТ МЕЖДУ ВЧЕРА И СЕГОДНЯ, не забудьте обработать None
            # ОТПРАВЬТЕ РАЗНИЦУ МЕЖДУ ФАЙЛАМИ В XCOM С КЛЮЧОМ file_difference МЫ БУДЕМ ИСПОЛЬЗОВАТЬ В СЛЕДУЮЩЕЙ ФУНКЦИИ
            file_difference = sorted(set(all_files_to_upload) - set(files_on_server))
            task_instance.xcom_push(key=FILE_DIFFERENCE, value=file_difference)

            # Отправляем разницу в Xcom с другим ключем
            # ОТПРАВЬТЕ СПИСОК ВСЕХ ФАЙЛОВ В XCOM С КЛЮЧОМ files_full МЫ БУДЕМ ИСПОЛЬЗОВАТЬ ЭТО В СЛЕДУЮЩЕМ ЗАПУСКЕ
            files_full = files_on_server + file_difference
            task_instance.xcom_push(key=FILES_FULL, value=files_full)
        else:
            raise AirflowException(f"Request failed {response.status_code}")
            
    # Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(url, table_name, client, **kwargs):
    
    # Получаем разницу в файлах сегодня и вчера 
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(task_ids='fetch_data_to_xcom', key=FILE_DIFFERENCE)

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (campaign String, cost Int64, date  String) ENGINE Log')
    
    # Итеративно проходимся по файлам и добавляем в ClickHouse
    for file in files:        
        # Чтение данных из CSV
        data_frame = pd.read_csv(url + file)  

        # Запись data frame в ClickHouse
        client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records')) 

# Создадим объект класса DAG
with DAG(
    dag_id='43342120_hm_512',
    tags=['43342120_hm'],
    schedule_interval='@daily', 
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 4),
) as dag:
    
    fetch_data_to_xcom = PythonOperator(
        task_id='fetch_data_to_xcom',
        python_callable=fetch_data_to_xcom,
        op_args = ['http://158.160.116.58:4009/files/'],
        dag=dag,
    )

    # Задачи для загрузки данных 
    upload_to_clickhouse = PythonOperator(
        task_id='upload_to_clickhouse',
        python_callable=upload_to_clickhouse,
        op_args = ['http://158.160.116.58:4009/download/', '43342120_hm_52_campaign_table', CH_CLIENT],
        dag=dag,
    )


    fetch_data_to_xcom >> upload_to_clickhouse
