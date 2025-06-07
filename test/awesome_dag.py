from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting data...")

def transform():
    print("Transforming data...")

def load():
    print("Loading data...")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 7),
    "catchup": False,
}

dag = DAG(
    "daily_etl",
    default_args=default_args,
    schedule_interval="@daily",
)

start = DummyOperator(task_id="start", dag=dag)
extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
load_task = PythonOperator(task_id="load", python_callable=load, dag=dag)
end = DummyOperator(task_id="end", dag=dag)

start >> extract_task >> transform_task >> load_task >> end