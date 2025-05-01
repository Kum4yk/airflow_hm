import os

from clickhouse_driver import Client  

from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago


DAG_ID = '43342120_hm_611'
TAGS = ['43342120_hm']


folder_path = "/usr/local/airflow/plugins/sql/"
with DAG(
    DAG_ID,
    tags=TAGS,
    schedule_interval=None, 
    start_date=days_ago(1),
) as dag:
    
    tasks = []
    for i, file_name in enumerate(os.listdir(folder_path)):
        
        select_query = open(folder_path + file_name, 'r').read()
        
        task = ClickHouseOperator(
            task_id=f"task_{i}",
            sql=f"CREATE VIEW {DAG_ID}_{i} as {select_query}",
            clickhouse_conn_id='clickhouse_default', 
        )
        
        tasks.append(task)

        if i:
            tasks[i-1] >> tasks[i]
