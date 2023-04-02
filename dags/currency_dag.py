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
    'populate_currency_table',
    default_args=default_args,
    schedule_interval='5 10 * * *',
)

def start_currency_dag():
    print('Starting currency DAG...')
    
def check_table_exists(currency):
    hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='mysqldb')
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{currency}'")
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result is not None
    
def create_table_if_not_exists():
    if not check_table_exists('currency'):
        hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='mysqldb')
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE currency (
                currency_name VARCHAR(255) NOT NULL,
                counry_code VARCHAR(255) PRIMARY KEY
            )
        """)
        cursor.close()
        connection.close()


def fetch_currencies():
    response = requests.get('http://country.io/currency.json')
    data = json.loads(response.text)
    return data
    
    
    
def write_to_currency_table():
    data = fetch_currencies()
    hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='mysqldb')
    sql = "INSERT INTO currency (counry_code, currency_name) VALUES (%s, %s)"
    for country_code, currency_name in data.items():
        hook.run(sql, parameters=(country_code, currency_name))

    
def end_currency_dag():
    print('Currency DAG completed.')
    
start_task = PythonOperator(
    task_id='start_currency_dag',
    python_callable=start_currency_dag,
    dag=dag
)


create_table_task = PythonOperator(
    task_id='create_currency_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)


write_table_task = PythonOperator(
    task_id='write_to_currency_table',
    python_callable=write_to_currency_table,
    dag=dag,
)


declaration_task = PythonOperator(
    task_id='end_currency_dag',
    python_callable=end_currency_dag,
    dag=dag,
)


start_task >> create_table_task >> write_table_task >> declaration_task