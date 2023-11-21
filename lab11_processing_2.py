from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from pandas import json_normalize
import json, os
 
# ham xu ly du lieu
def _process_user(ti):
    user = ti.xcom_pull(task_ids='extracting_user')
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] 
    })
    os.remove('./data/processed_user.csv')
    processed_user.to_csv('./data/processed_user.csv', index=None, header=False)


# tao DAG
with DAG('lab11_processing_2', start_date=datetime(2023, 11, 1),
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
    storing_user = BashOperator(
        task_id='storing_user',
        bash_command='''
        path='/home/vpnsaigon/airflow/data'
        while IFS=, read -r fn ls ct us pa em; do
            sql="INSERT OR REPLACE INTO lab11 (firstname, lastname, country, username, password, email) VALUES ('$fn','$ls','$ct','$us','$pa','$em')"
            sqlite3 $path/lab11_users.db "$sql"
        done < $path/processed_user.csv
        '''
    )

    # sap xep thu tu task
    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user

