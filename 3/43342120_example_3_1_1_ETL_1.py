# Библиотеки для работы с XML, http, data frame и date
import requests as req
import pandas as pd
import xml.etree.ElementTree as ET
from clickhouse_driver import Client

# Библиотеки для работы с Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, date, s_file):
    
    # Выполняем GET-запрос для получения данных за указанную дату
    request = req.get(f"{url}?date_req={date}")  
    with open(s_file, "w", encoding="utf-8") as tmp_file:
        tmp_file.write(request.text)  # Записываем текст ответа в файл

# Функция для обработки данных в формате XML и преобразования их в CSV
def transform_data(s_file, csv_file, date):
    
    rows = list()  # Список для хранения значений из XML
    
    # Парсинг XML дерева
    parser = ET.XMLParser(encoding="utf-8")
    tree = ET.parse(s_file, parser=parser).getroot()
    
    # Получение необходимых значений
    for child in tree.findall("Valute"):
        num_code = child.find("NumCode").text
        char_code = child.find("CharCode").text
        nominal = child.find("Nominal").text
        name = child.find("Name").text
        value = child.find("Value").text
        
        # Добавление одной записи в список для последующих преобразований
        rows.append((num_code, char_code, nominal, name, value)) 

    # Считывание полученного списка в Data Frame, добавление даты и запись в CSV файл
    data_frame = pd.DataFrame(
        rows, columns=["num_code", "char_code", "nominal", "name", "value"]
    )
    data_frame['date'] = date 
    data_frame.to_csv(csv_file, sep=",", encoding="utf-8", index=False) 

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client):
    
    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)  
    
    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (num_code Int64, char_code String, nominal Int64, name String, value String, date String) ENGINE Log')
    
    # Запись data frame в ClickHouse
    client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records')) 


CH_CLIENT = Client(
    host='158.160.116.58',  # IP-адрес сервера ClickHouse
    user='student',  # Имя пользователя для подключения
    password='dfqh89fhq8',  # Пароль для подключения
    database='sandbox'  # База данных, к которой подключаемся
)

TABLE_NAME = '43342120_example_3_1_1_ETL_1'

# Определяем DAG, это контейнер для описания нашего пайплайна
with  DAG(
    dag_id='43342120',
    schedule_interval='@daily',      # Как часто запускать, счит. CRON запись
    start_date=days_ago(1), # Начало и конец загрузки (такая запись всегад будет ставить вчерашний день)
    tags=['examples']
) as dag:

    # Задача для извлечения данных 
    task_extract = PythonOperator(
        task_id='extract_data',          # Уникальное имя задачи
        python_callable=extract_data,    # Функция которая будет запущена (определена выше)
        
        # Параметры в виде списка которые будут переданы в функцию "extract_data"
        op_args=['http://www.cbr.ru/scripts/XML_daily.asp', '01/01/2024', './extracted_data.xml'],
        dag=dag,                         # DAG к которому приклеплена задача
    )

    # Задачи для преобразования данных 
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        # Передача аргументов через словарь, а не список
        op_kwargs = {
            's_file': './extracted_data.xml', 
            'csv_file': './transformed_data.csv', 
            'date': '01/01/2024'},
        dag=dag,
    )

    # Задачи для загрузки данных 
    task_upload = PythonOperator(
        task_id='upload_to_clickhouse',
        python_callable=upload_to_clickhouse,
        op_args = ['./transformed_data.csv', TABLE_NAME, CH_CLIENT],
        dag=dag,
    )

    # Связываем задачи в соответствующих дагах. Посмотреть связь можно здесь 
    task_extract >> task_transform >> task_upload
