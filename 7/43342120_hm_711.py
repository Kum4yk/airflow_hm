DAG_ID = '43342120_hm_711'
TAGS = ['43342120_hm']
meta_kwargs = {'dag_id': DAG_ID, 'tags': TAGS}


import json
import requests
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.telegram.operators.telegram import TelegramOperator


DATE_COL = 'date'
CURRENCY_SOURCE_COL = 'currency_source'
CURRENCY_COL = 'currency'
VALUE_COL = 'value'

HOST_EXCR = BaseHook.get_connection("exchange_rate").host
PASSWORD_EXCR = BaseHook.get_connection("exchange_rate").password 

exchange_rate_dct = Variable.get('exchange_rate', deserialize_json=True)
json_file_path = exchange_rate_dct['s_file']
data_file_path = exchange_rate_dct['csv_file']

currency_source = 'USD'


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


def upload_to_clickhouse(csv_file_path, table_name):
    """
    Считывание CSV файл, создание таблицу в базе данных ClickHouse и добавление данных в неё.
    
    :param csv_file_path: Путь до файла с данными.
    :param table_name: Имя таблицы в БД.
    :param client: Клиент подключения к ClickHouse.
    """
    
    df: pd.DataFrame = pd.read_csv(csv_file_path)

    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute(f'INSERT INTO {table_name} VALUES', df.to_dict('records'))


def send_callback(context):
    send_message = TelegramOperator(
        task_id='send_message',
        telegram_conn_id='telegram_43342120',
        chat_id='-1002520413335',
        text=f'Some error in {DAG_ID} dag',
        dag=dag,
    )
    return send_message.execute(context=context)


default_args = {
    'on_failure_callback': send_callback,
}

with DAG(
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024,1,1),
    end_date=datetime(2024,1,10),
    max_active_runs=1,  # Будет запускать 1 DAG в момент времени, чтобы ограничить коллизии
    **meta_kwargs,
) as dag:
    
    get_api_data = PythonOperator(
        task_id='get_api_data',
        python_callable=get_api_exchangerate_data,
        op_kwargs={
            'day': '{{ ds }}',
            'currency_source': currency_source,
            'access_key': PASSWORD_EXCR,
            'file_path': json_file_path,
            'api_url': HOST_EXCR,
        },
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_exchangerate_data,
        op_kwargs={
            'src_file_path': json_file_path,
            'dst_file_path': data_file_path,
        },
    )

    create_exchange_table = ClickHouseOperator(
        task_id='create_exchange_table',
        sql=f'CREATE TABLE IF NOT EXISTS {DAG_ID}_exchange (date String, currency_source String, currency String, value Float32) ENGINE Log',
        clickhouse_conn_id='clickhouse_default', 
    )

    create_order_table = ClickHouseOperator(
        task_id='create_order_table',
        sql=f'CREATE TABLE IF NOT EXISTS {DAG_ID}_order (date String, order_id Int64, purchase_rub Float64, purchase_usd Float64) ENGINE Log',
        clickhouse_conn_id='clickhouse_default', 
    )

    upload_exchange_data = PythonOperator(
        task_id='upload_exchange_data',
        python_callable=upload_to_clickhouse,
        op_kwargs={
            'csv_file_path': data_file_path,
            'table_name': f'{DAG_ID}_exchange', 
        },
    )

    join_tables = ClickHouseOperator(
        task_id='join_tables',
        sql="""
            INSERT INTO 43342120_hm_711_order
                SELECT 
                    date
                    , order_id
                    , purchase_rub
                    , purchase_rub * value as purchase_usd
                FROM airflow.orders t1
                LEFT JOIN (
                    SELECT * 
                    FROM sandbox.43342120_hm_711_exchange
                    WHERE currency = 'RUB' and date = '{{ds}}'
                ) t2 on t1.date = t2.date
                WHERE date = '{{ds}}';
        """,
        clickhouse_conn_id='clickhouse_default', 
    )

    get_api_data >> transform_data >>  [create_exchange_table, create_order_table] >> upload_exchange_data >> join_tables
