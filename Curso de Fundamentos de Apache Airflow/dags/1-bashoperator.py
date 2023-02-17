from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='bash_operator',
         description='Utilizando bash operator',
         start_date=datetime(2023, 2, 16)) as dag:
        t1 = BashOperator(task_id='hello_with_bash',
                          bash_command="echo 'hola gente de platzi'")