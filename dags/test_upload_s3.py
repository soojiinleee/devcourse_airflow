from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3 (filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_s3_conn_id')
    hook.load_file(
        filename=filename,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )

with DAG (
    dag_id='upload_to_s3',
    schedule_interval=None,
    start_date=datetime(2025, 2, 2),
    catchup=False
) as dag:
    upload = PythonOperator(
        task_id='upload',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename':'/opt/airflow/data/test.csv',
            'key': 'KOPIS/test.csv',
            'bucket_name': 'data-eng-practices'
        }
    )