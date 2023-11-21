from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator

# cac ham xu ly
def _task_select_day(ti):
    tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    today = tabDays[datetime.now().weekday()]
    ti.xcom_push(key='my_key', value=today)

def _branch(ti):
    day = ti.xcom_pull(key='my_key', task_ids='task_select_day')
    if day == 'monday':
        return 'task_for_monday'
    elif day == 'tuesday':
        return 'task_for_tuesday'
    elif day == 'wednesday':
        return 'task_for_wednesday'
    elif day == 'thursday':
        return 'task_for_thursday'
    elif day == 'friday':
        return 'task_for_friday'
    elif day == 'saturday':
        return 'task_for_saturday'
    elif day == 'sunday':
        pass


# Khoi tao DAG
dag = DAG(
    dag_id = 'lab13_branch',
    start_date=datetime(2023, 11, 1),
    schedule_interval='@daily',
    catchup=False
)

# Khoi tao Task
task_select_day = PythonOperator(
    task_id = 'task_select_day',
    python_callable=_task_select_day,
    dag=dag
)

branching = BranchPythonOperator(
    task_id = 'branching',
    python_callable = _branch,
    dag=dag
)


task_for_monday = BashOperator(
    task_id = 'task_for_monday',
    bash_command='echo "task_for_monday"',
    dag=dag
)

task_for_tuesday = BashOperator(
    task_id = 'task_for_tuesday',
    bash_command='echo "task_for_tuesday"',
    dag=dag
)

task_for_wednesday = BashOperator(
    task_id = 'task_for_wednesday',
    bash_command='echo "task_for_wednesday"',
    dag=dag
)

task_for_thursday = BashOperator(
    task_id = 'task_for_thursday',
    bash_command='echo "task_for_thursday"',
    dag=dag
)

task_for_friday = BashOperator(
    task_id = 'task_for_friday',
    bash_command='echo "task_for_friday"',
    dag=dag
)

task_for_saturday = BashOperator(
    task_id = 'task_for_saturday',
    bash_command='echo "task_for_saturday"',
    dag=dag
)

# sap xep thuc tu branch
task_select_day >> branching >> [task_for_monday, task_for_tuesday, task_for_wednesday, \
                                 task_for_thursday, task_for_friday, task_for_saturday]

