from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime
from random import randint


RANDOM_KEY = 'rand'


def rand(**kwargs):
    kwargs['ti'].xcom_push(key=RANDOM_KEY, value=randint(0, 10))


def branch(**kwargs):
    """Если > 5 то вернуть higher, иначе вернуть lower"""
    if kwargs['ti'].xcom_pull(key=RANDOM_KEY) > 5:
        return 'higher'
    return 'lower'


default_args = {
    'owner':'airflow',
    'start_date': datetime(2022, 2, 16)
}


with DAG(
    dag_id='43342120_hm_511',
    tags=['43342120_hm'],
    schedule_interval=None,
    default_args=default_args,
) as dag:

    random_number = PythonOperator(
        task_id = 'random_number',
        python_callable=rand,
    )

    branch_op = BranchPythonOperator(
        task_id = 'branch_task',
        python_callable=branch,
    )
    
    lower = DummyOperator(
        task_id = 'lower',
    )

    higher = DummyOperator(
        task_id = 'higher',
    )

    random_number >> branch_op >> [lower, higher]
