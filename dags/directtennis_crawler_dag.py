from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {}

def run_crawler():
    import sys, os
    from datetime import datetime, timedelta

    import DirectTennisCrawler as cr

    cr.runCrawler()


with DAG('crawler_dag', schedule_interval='1 0 1 * *', start_date=datetime(2024, 11, 1),
         catchup=False) as dag:

    run_crawler = PythonOperator(
        task_id='run_crawler',
        python_callable=run_crawler
    )

    error_alert = EmailOperator(
        task_id='error_alert',
        to=['hylau17@yahoo.com'],
        subject='Crawler Error',
        html_content=f'There is an error in Direct Tennis Crawler run at {datetime.now()}',
        trigger_rule='one_failed'
    )

    sucess = DummyOperator(
        task_id='success',
        trigger_rule='all_success'
    )

    run_crawler >> [error_alert, sucess]