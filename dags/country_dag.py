from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import requests
import json
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'kartaca',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'populate_country_table',
    default_args=default_args,
    schedule_interval='0 10 * * *',
)


def start_country_dag():
    print('Starting country DAG...')
    
    
    
def check_table_exists(country):
    hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='mysqldb')
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{country}'")
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result is not None
    
def create_table_if_not_exists():
    if not check_table_exists('country'):
        hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='mysqldb')
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE currency (
                country_name VARCHAR(255) NOT NULL,
                counry_code VARCHAR(255) PRIMARY KEY
            )
        """)
        cursor.close()
        connection.close()
    
def fetch_countries():
    response = requests.get('http://country.io/names.json')
    data = json.loads(response.text)
    return data
    
def write_to_currency_table():
    data = fetch_currencies()
    hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='mysqldb')
    sql = "INSERT INTO country (counry_code, country_name) VALUES (%s, %s)"
    for country_code, country_name in data.items():
        hook.run(sql, parameters=(country_code, country_name))
    
def end_country_dag():
    print('Country DAG completed.')
    
start_task = PythonOperator(
    task_id='start_country_dag',
    python_callable=start_country_dag,
    dag=dag
)


create_table_task = PythonOperator(
    task_id='create_country_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)


write_table_task = PythonOperator(
    task_id='write_to_country_table',
    python_callable=write_to_country_table,
    dag=dag,
)


declaration_task = PythonOperator(
    task_id='end_country_dag',
    python_callable=end_country_dag,
    dag=dag,
)


start_task >> create_table_task >> write_table_task >> declaration_task