from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print('Hello gente de platzi')

with DAG(dag_id='python_operator',
         description='Nuestro primer DAG utilizando python operator',
         schedule_interval='@once',
         start_date=datetime(2022, 2, 16)) as dag:
    t1 = PythonOperator(task_id='hello_with_python',
                        python_callable=print_hello)