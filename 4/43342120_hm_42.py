from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


# Определяем параметры DAG
dag = DAG(
    '43342120_hm_42',
    tags=['43342120_hm'],
    schedule_interval=None,  # Запускать вручную
    start_date=days_ago(1),  
)

# Создание задачи для выполнения SQL-запроса
create_table_task = PostgresOperator(
    task_id='run_sql',
    postgres_conn_id='postgres',
    sql=""" select count(1) from dag """,
    dag=dag,
)
