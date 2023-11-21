from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import json, os
import sqlite3
 
# ham xu ly du lieu
def _process_user(ti):
    user = ti.xcom_pull(task_ids='extracting_user')
    user = user['results'][0]
    processing_user = {
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] 
    }
    os.remove('./data/processing_user.json')
    processing_user = json.dumps(processing_user)
    with open('./data/processing_user.json', 'w') as f:
        f.write(processing_user)

# ham luu tru du lieu 
def _storing_user():
    conn = sqlite3.connect('/home/vpnsaigon/airflow/data/lab11_users.db')

    # Prepare the data to be inserted
    with open('./data/processing_user.json', 'r') as f:
        data = f.read().strip()
        data = json.loads(data)

    # Insert the data into the SQLite database
    columns = ', '.join(data.keys())
    placeholders = ', '.join('?' * len(data))
    values = [x for x in data.values()]
    query = 'INSERT OR REPLACE INTO lab11 (%s) VALUES (%s)' % (columns, placeholders)

    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    cursor.close()

# tao DAG
with DAG('lab11_processing', start_date=datetime(2023, 11, 1),
        schedule_interval='@daily', catchup=True) as dag:
    
    # tao table trong database
    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='sqlite_lab11',
        sql='''
            CREATE TABLE IF NOT EXISTS lab11 (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
            );
        '''
    )

    # kiem tra api
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    # request api
    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # xu ly du lieu
    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_process_user
    )

    # luu tru du lieu 
    storing_user = PythonOperator(
        task_id='storing_user',
        python_callable=_storing_user
    )

    # sap xep thu tu task
    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user

