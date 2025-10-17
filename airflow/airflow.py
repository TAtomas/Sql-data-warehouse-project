from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_bronze_load():
    subprocess.run(
        ["python", r"/opt/airflow/dags/Bronze_load.py"],
        check=True
    )

def run_silver_load():
    subprocess.run(
        ["python", r"D:\DWH_Project\DataOF DWH Braa Project\airflow\Silver_load.py"],
        check=True
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'dwh_load_pipeline',
    default_args=default_args,
    description='Load Bronze then Silver data',
    schedule_interval=None,
    start_date=datetime(2025, 10, 17),
    catchup=False,
    tags=['DWH'],
) as dag:

    bronze_task = PythonOperator(
        task_id='load_bronze',
        python_callable=run_bronze_load
    )

    silver_task = PythonOperator(
        task_id='load_silver',
        python_callable=run_silver_load
    )

    bronze_task >> silver_task
