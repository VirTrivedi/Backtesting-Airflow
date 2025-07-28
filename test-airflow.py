from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello, World!")
    return "Success"

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule=timedelta(days=1),
    catchup=False
)

task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag
)