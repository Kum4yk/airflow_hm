from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


DAG_ID = '43342120_hm_531'
TAGS = ['43342120_hm']

REPORT_ID = 'report_id'


def response_operator(response, **kwargs):
    dct = response.json()
    if REPORT_ID in dct:
        kwargs['ti'].xcom_push(key=REPORT_ID, value=dct[REPORT_ID])
        return True


def response_sensor(response, **kwargs):
    return response.json()['message'] == 'The report is ready!'


with DAG(
    dag_id=DAG_ID,
    tags=TAGS,
    schedule_interval=None,  # Запускать вручную
    start_date=days_ago(1)
) as dag:
# HTTP-оператор для отправки запроса на создание отчёта
    start_report_task = SimpleHttpOperator(
        task_id='start_report_task',
        http_conn_id='report_api', 
        endpoint='start_report', 
        method='GET',
        response_check=response_operator,  # Проверка на наличие Report ID
        dag=dag,
    )

    # HTTP-сенсор для проверки готовности отчёта
    check_report_task = HttpSensor(  
        task_id='check_report_task',
        http_conn_id='report_api',  # Укажите ваше соединение
        # ВАШ КОД, НЕОБХОДИМО ВЫТАЩИТЬ из XCOM report_id используйте jinja
        endpoint='check_report/' + f"{{{{ task_instance.xcom_pull(key='{REPORT_ID}') }}}}",
        response_check=response_sensor, # Проверка что отчет готов
        method='GET',
        poke_interval=2,  
        timeout=60,  
        mode='poke',  
        dag=dag,
    )

    # Определение порядка выполнения задач
    start_report_task >> check_report_task