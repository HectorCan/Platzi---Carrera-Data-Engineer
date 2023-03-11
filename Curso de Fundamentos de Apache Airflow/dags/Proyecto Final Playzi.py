'''
  Curso de fundamentos de Apache Airflow

  Proyecto Final: Platzi explora el espacio

  @author: Hector Can
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from notificationoperator import NotificationOperator

from datetime import datetime


def collect_satellite_data(**context):
  import pandas as pd

  data = pd.DataFrame({
      "student": ["Hector Can", "Sergio Can"],
      "timestamp": [context['logical_date'], context['logical_date']]
  })

  data.to_csv(f"/tmp/platzi_data_{context['ds_nodash']}.csv", header=True)


with DAG(dag_id='platzi-explore-the-space',
         description='Proyecto final del curso de fundamentos de apache airflow',
         schedule_interval='@daily',
         start_date=datetime(2023, 2, 1),
         end_date=datetime(2023, 2, 15),
         max_active_runs=1) as dag:

    request_nasa_auth = BashOperator(task_id='request-nasa-auth',
                                     bash_command='sleep 20 && echo "OK" > /tmp/response_{{ds_nodash}}.txt')

    nasa_authorized = FileSensor(task_id='nasa-authorized',
                                 filepath='/tmp/response_{{ds_nodash}}.txt')

    get_spacex_data = BashOperator(task_id='get-spacex-data',
                                   bash_command='curl -o /tmp/history.json -L "https://api.spacexdata.com/v4/history"')

    get_satellite_data = PythonOperator(task_id='get-satellite-data',
                                        python_callable=collect_satellite_data)

    notify_marketing = NotificationOperator(task_id='notify-marketing',
                                            type='marketing')

    notify_analyst = NotificationOperator(task_id='notify-analyst',
                                          type='analyst')

    request_nasa_auth >> nasa_authorized >> get_spacex_data >> get_satellite_data >> [
        notify_marketing, notify_analyst]
