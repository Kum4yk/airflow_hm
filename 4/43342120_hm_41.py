from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


# Создаем DAG
dag = DAG(
    '43342120_hm_41',
    tags=['43342120_hm'],
    schedule_interval=None,  # Запускать вручную
    start_date=datetime(2024, 1, 1),
)

# Задачи с ограничением на параллельное выполнение через пул
task_1 = BashOperator(
    task_id='task_1',
    pool='one_task_pool',  # Указываем пул для ограничения параллельности
    bash_command="sleep 3",
    priority_weight=6,
    dag=dag,
)

task_2 = BashOperator(
    task_id='task_2',
    pool='one_task_pool',
    bash_command="sleep 3",
    priority_weight=4,
    dag=dag,
)

task_3 = BashOperator(
    task_id='task_3',
    pool='one_task_pool',
    bash_command="sleep 3",
    priority_weight=2,
    dag=dag,
)

task_11 = BashOperator(
    task_id='task_11',
    pool='one_task_pool',  # Указываем пул для ограничения параллельности
    bash_command="sleep 3",
    priority_weight=5,
    dag=dag,
)

task_22 = BashOperator(
    task_id='task_22',
    pool='one_task_pool',
    bash_command="sleep 3",
    priority_weight=3,
    dag=dag,
)

task_33 = BashOperator(
    task_id='task_33',
    pool='one_task_pool',
    bash_command="sleep 3",
    priority_weight=1,
    dag=dag,
)

task_1 >> task_11
task_2 >> task_22
task_3 >> task_33