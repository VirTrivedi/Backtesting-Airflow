from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os

def hello_world():
    print("Hello, World!")
    return "Success"

def test_s3_connection():
    """Test S3 connection and list buckets"""
    try:
        s3 = boto3.client('s3')
        buckets = s3.list_buckets()
        print("S3 connection successful!")
        print("Available buckets:", [b['Name'] for b in buckets['Buckets']])
        return "S3 connection successful"
    except Exception as e:
        print(f"S3 connection failed: {e}")
        return f"S3 connection failed: {e}"

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

hello_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag
)

s3_test_task = PythonOperator(
    task_id='test_s3_connection',
    python_callable=test_s3_connection,
    dag=dag
)

hello_task >> s3_test_task