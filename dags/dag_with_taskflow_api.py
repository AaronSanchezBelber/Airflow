from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    'owner': 'sosp',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api_v1',
     default_args=default_args,
     start_date=datetime(2021, 10, 26),
     schedule_interval='@daily')

def hello_world_etl():
    
    @task()
    def get_name():
        return 'SSopss'
    
    @task()
    def get_age():
        return 27
    
    @task()
    def greet(name, age):
        print(f"Hello world! Mi name is {name} and I am {age} years old")

    name = get_name()
    age = get_age()
    greet(name=name, age=age)

    greet_dag = hello_world_etl()