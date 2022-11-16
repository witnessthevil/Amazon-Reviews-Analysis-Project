import datetime
#from dags_import.transformer import Transformer
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator 

def task1():
    pass

with DAG(
    dag_id="whatthefuck",
    schedule_interval="0 0 * * *",
    start_date=datetime.datetime.today(),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:

    task_1 = BashOperator(
        task_id = 'task_1',
        bash_command='echo rule 34'
    )

    task_1 