from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='5.1-orquestation',
         description='Probando la orquestacion',
         schedule_interval='@daily',
         start_date=datetime(2023, 2, 1),
         end_date=datetime(2023, 3, 1),
         catchup=True, # Execute previous days start to today
         max_active_runs=1,
         # This makes that each day is run one per one, and not in parallel
         default_args={"depends_on_past": True}) as dag:
    t1 = BashOperator(task_id='tarea1',
                      bash_command='sleep 2 && echo "Tarea 1"')
    t2 = BashOperator(task_id='tarea2',
                      bash_command='sleep 2 && echo "Tarea 2"')
    t3 = BashOperator(task_id='tarea3',
                      bash_command='sleep 2 && echo "Tarea 3"')
    t4 = BashOperator(task_id='tarea4',
                      bash_command='sleep 2 && echo "Tarea 4"')
    
    t1 >> t2 >> [t3, t4]
    