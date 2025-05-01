from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

dag = DAG(
    '43342120_hm_322',
    tags=['43342120_hm'],
    schedule_interval=timedelta(days=1), 
    start_date=days_ago(1),
)


# Функция которая всегда верна
def success():
  pass

# Функция которая скипает задачу
def skip():
  raise AirflowSkipException

# Функция которая падает с ошибкой
def failed():
  raise AirflowFailException

task_0 = PythonOperator(
  task_id='task_0',
  python_callable=success,
  dag=dag
)

task_1 = PythonOperator(
  task_id='task_1',
  python_callable=skip,
  dag=dag
)

task_2 = PythonOperator(
  task_id='task_2',
  python_callable=failed,
  dag=dag
)

task_3 = PythonOperator(
  task_id='task_3',
  trigger_rule='one_success',
  python_callable=lambda: print("Success"),
  dag=dag
)

[task_0, task_1, task_2] >> task_3



task_4 = PythonOperator(
  task_id='task_4',
  python_callable=success,
  dag=dag
)

task_5 = PythonOperator(
  task_id='task_5',
  python_callable=failed,
  dag=dag
)

task_6 = PythonOperator(
  task_id='task_6',
  python_callable=failed,
  dag=dag
)

task_7 = PythonOperator(
  task_id='task_7',
  python_callable=lambda: print("Success"),
  trigger_rule='one_failed',
  dag=dag
)

[task_4, task_5,task_6] >> task_7
