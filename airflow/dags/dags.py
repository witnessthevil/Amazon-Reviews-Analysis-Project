import datetime
from email.mime import application
from json import load
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
import concurrent.futures
from airflow.utils.dates import days_ago
import os
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG(
    dag_id="spark-configer",
    schedule_interval='@once',
    start_date=days_ago(2),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    max_active_runs=1
) as dag:

    task_1 = BashOperator(
        task_id = 'task_1',
        bash_command='python3 /opt/airflow/tasks/extractor.py',
        dag=dag
    )

    task_2 = SparkSubmitOperator(
        task_id = 'task_2', 
        conn_id = 'spark-conn',
        application = '/opt/airflow/tasks/transformer.py',
        dag=dag
    )

    task_3 = BashOperator(
        task_id = 'task_3',
        bash_command='python3 /opt/airflow/tasks/load.py',
        dag=dag
    )

    task_1 >> task_2 >> task_3 