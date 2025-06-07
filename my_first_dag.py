from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end
