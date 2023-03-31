from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args =  {
    'owner': 'aaron',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids ='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids ='get_name', key='last_name')
    age = ti.xcom_pull(task_ids ='get_age', key=27)
    print(f'hello world!. My name is {first_name} {last_name}, and I am {age} years old!')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Aarron')
    ti.xcom_push(key='last_name', value='San')

def get_age(ti):
    ti.xcom_push(key='age', value= 27)


with DAG (
    default_args = default_args,
    dag_id = '__our_dag_python____',
    start_date = datetime(2021, 10 , 6),
    schedule_interval = '@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable = greet,
        # op_kwargs={'age': 27}
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name,
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable = get_age,
    )

    task2 >> task1 >> task3