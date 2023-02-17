from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(dag_id='primerdag',
         description='Nuestro primer DAG',
         start_date=datetime(2023, 2, 16),
         schedule_interval='@once') as dag:
    t1 = EmptyOperator(task_id='dummy')