from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id= 'our_first_dag_4',
    default_args=default_args,
    description= 'This is my firts dag',
    start_date= datetime(2021, 7, 29),
    schedule_interval='@daily'
) as dag:
    task1 =  BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is the first task!'

    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command= 'i will run after task 1'
    )

    task3 = BashOperator(
        task_id='3_task',
        bash_command= 'i will run after task 1 at the same time as task 2'
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    task1 >> task2
    task1 >> task3