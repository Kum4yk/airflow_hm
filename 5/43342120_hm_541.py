"""
https://t.me/+eoB7jThOh4M5Y2Iy
telegram_43342120
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime
from airflow.exceptions import AirflowException


DAG_ID = '43342120_hm_541'
TAGS = ['43342120_hm']


def send_callback(context):
    send_message = TelegramOperator(
        task_id='send_message',
        telegram_conn_id='telegram_43342120',
        chat_id='-1002520413335',
        text=f'Some error in {DAG_ID}',
        dag=dag,
    )
    return send_message.execute(context=context)

def raise_exc():
    raise AirflowException


default_args = {
    'on_failure_callback': send_callback,
}

with DAG(
    dag_id=DAG_ID,
    tags=TAGS,
    default_args=default_args,
    schedule_interval=None,  # Только ручной запуск
    start_date=datetime(2023, 1, 1)
) as dag:
    
    # PythonOperator, который выбрасывает исключение
    exception_node = PythonOperator(
        task_id='exception_node',
        python_callable=raise_exc,
    )

    exception_node