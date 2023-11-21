from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from google_drive_downloader import GoogleDriveDownloader as gdd
import os

# file path
questions_csv_path = '/home/vpnsaigon/airflow/data_asm2/Questions.csv'
answers_csv_path = '/home/vpnsaigon/airflow/data_asm2/Answers.csv'

# Cac ham xu ly
def _branching():
    if os.path.exists(answers_csv_path) and os.path.exists(questions_csv_path):
        return 'clear_file'
    return 'end'

def _download_question_file_task():
    # gdd.download_file_from_google_drive(file_id='1mkm0X1pTtKZCl8DVp77KhRRTQ5-zqvXQ',
    #                                 dest_path=questions_csv_path,
    #                                 unzip=True)
    pass

def _download_answer_file_task():
    # gdd.download_file_from_google_drive(file_id='1A_lchdpY3L5dQ-3T7DdnTUyGOMP2Ha_S',
    #                                 dest_path=answers_csv_path,
    #                                 unzip=True)
    pass


# Định nghĩa DAG
dag = DAG(
    dag_id='asm02_airflow_bigdata_2',
    start_date=datetime(2023, 11, 10),
    description='Airflow and BigData',
    schedule_interval='@daily',
    catchup=False
)

# Định nghĩa các Task
start = EmptyOperator(
	task_id='start',
    dag=dag
)

end = EmptyOperator(
	task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

branching = BranchPythonOperator(
    task_id = 'branching',
    python_callable=_branching,
    dag=dag
)

clear_file = BashOperator(
    task_id = 'clear_file',
    # bash_command=f'''
    #     data_path='/home/vpnsaigon/airflow/data_asm2'
    #     if [ -d $data_path ]; then
    #         files=$(ls $data_path)
    #         for file in $files; do
    #             rm -rf "$data_path/$file"
    #         done
    #     fi
    # ''',
    bash_command='',
    dag=dag
)

download_question_file_task = PythonOperator(
    task_id = 'download_question_file_task',
    python_callable=_download_question_file_task,
    dag=dag
)

download_answer_file_task = PythonOperator(
    task_id = 'download_answer_file_task',
    python_callable=_download_answer_file_task,
    dag=dag
)

import_questions_mongo = BashOperator(
    task_id = 'import_questions_mongo',
    bash_command=f'mongoimport --type csv -d dep303_asm02 -c Questions --headerline --drop /home/vpnsaigon/airflow/data_asm2/Questions.csv',
    dag=dag 
)

import_answers_mongo = BashOperator(
    task_id = 'import_answers_mongo',
    bash_command=f'mongoimport --type csv -d dep303_asm02 -c Answers --headerline --drop /home/vpnsaigon/airflow/data_asm2/Answers.csv',
    dag=dag
)

spark_process = BashOperator(
    task_id = 'spark_process',
    bash_command=f'python3 /home/vpnsaigon/airflow/dags/asm02_spark_submit.py',
    dag=dag
)

import_output_mongo = BashOperator(
    task_id = 'import_output_mongo',
    bash_command=f'''
        x=$(find /home/vpnsaigon/airflow/data_asm2/outputs -type f -name "*.csv")
        mongoimport --type csv -d dep303_asm02 -c Outputs --headerline --drop $x
    ''',
    dag=dag
)

# Thiết lập phụ thuộc giữa các Task

start.set_downstream(branching)

branching.set_downstream(clear_file)
branching.set_downstream(end)

clear_file.set_downstream(download_answer_file_task)
clear_file.set_downstream(download_question_file_task)

download_answer_file_task.set_downstream(import_answers_mongo)
download_question_file_task.set_downstream(import_questions_mongo)

import_answers_mongo.set_downstream(spark_process)
import_questions_mongo.set_downstream(spark_process)

spark_process.set_downstream(import_output_mongo)
import_output_mongo.set_downstream(end)

###

