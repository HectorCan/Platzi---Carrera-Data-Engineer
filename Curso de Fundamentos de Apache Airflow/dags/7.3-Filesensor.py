from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

with DAG(dag_id='7.3-filesensor',
         description='Probando el File Sensor',
         schedule_interval='@daily',
         start_date=datetime(2023, 2, 1),
         end_date=datetime(2023, 2, 3),
         max_active_runs=1) as dag:
    t1 = BashOperator(task_id='creating_file',
                      bash_command='sleep 10 && touch /tmp/file.txt')
    
    t2 = FileSensor(task_id='waiting_file',
                    filepath='/tmp/file.txt')
    t3 = BashOperator(task_id='end-task',
                      bash_command="echo 'el fichero ha llegado'")
    
    t1 >> t2 >> t3