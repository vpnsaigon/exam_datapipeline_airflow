from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Định nghĩa DAG
default_args = {
    'ower': 'airflow',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='lab12_pipeline_parallel',
    default_args=default_args,
    description='Pipeline Parallel',
    schedule_interval='@daily',
    catchup=False
)

# Định nghĩa các Task

task_1 = BashOperator(
	task_id='task_1',
	bash_command='sleep 3',
    dag=dag
)

task_2 = BashOperator(
	task_id='task_2',
	bash_command='sleep 3',
    dag=dag
)

task_3 = BashOperator(
	task_id='task_3',
	bash_command='sleep 3',
    dag=dag
)

task_4 = BashOperator(
	task_id='task_4',
	bash_command='sleep 3',
    dag=dag
)


# Thiết lập phụ thuộc giữa các Task

task_1 >> [task_2, task_3] >> task_4


