from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.hooks.base_hook import BaseHook

from datetime import datetime


DAG_ID = '43342120_hm_521'
TAGS = ['43342120_hm']


HOST_EXCR = BaseHook.get_connection("exchange_rate").host
# https://api.exchangerate.host/timeframe'
PASSWORD_EXCR = BaseHook.get_connection("exchange_rate").password 
# 422bab0c9e08a5476912061877017b8d
currency_source = 'USD'


CREATE_TABLE_QUERY = f"CREATE TABLE IF NOT EXISTS {DAG_ID} (date String, currency_source String, currency String, value Float32) ENGINE Log"
date = '{{ ds }}'

# SQL-запрос, вставка данных в нашу таблицу после того как мы их выгрузим из API
INSERT_DATA_QUERY = """ INSERT INTO 43342120_hm_521
SELECT '{{ds}}' AS date, currency_source, currency, value
FROM (
    SELECT arrayJoin(splitByChar(',', replaceAll(replaceAll(replaceAll(visitParamExtractRaw(column, '{{ds}}'), '{', ''), '}', ''), '"', ''))) AS col1,
        LEFT(col1, 3) AS currency_source,
        SUBSTRING(col1, 4, 3) AS currency,
        SUBSTRING(col1, 8) AS value
    FROM url('https://api.exchangerate.host/timeframe?access_key=422bab0c9e08a5476912061877017b8d&start_date={{ds}}&end_date={{ds}}&source=USD'
, LineAsString, 'column String')
)
"""


with DAG(
    dag_id=DAG_ID,
    tags=TAGS,
    description='Пример использования ClickHouseOperator',
    schedule_interval='@daily',  # Запускать вручную
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 4)
) as dsg:

    # Оператор для создания таблицы
    create_table = ClickHouseOperator(
        task_id='create_table',
        sql=CREATE_TABLE_QUERY,
        clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    )

    # Оператор для вствыки данных в таблицу
    insert_data = ClickHouseOperator(
        task_id='insert_data',
        sql=INSERT_DATA_QUERY,  # SQL запрос, который нужно выполнить
        clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    )

    create_table >> insert_data
