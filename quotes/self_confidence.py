from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import requests
import psycopg2

# Default arguments for the DAG
default_args = {
    'owner': 'ike',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Fetch email settings and recipients from Airflow Variables
EMAIL_RECIPIENTS = Variable.get("self_confidence_recipients", deserialize_json=True)
EMAIL_CONFIG = {
    'smtp_host': Variable.get("smtp_host"),
    'smtp_port': Variable.get("smtp_port"),
    'smtp_user': Variable.get("smtp_user"),
    'smtp_password': Variable.get("smtp_password"),
    'smtp_mail_from': Variable.get("smtp_mail_from"),
}

# Define the DAG
with DAG(
    dag_id='self_confidence_quote_etl',
    default_args=default_args,
    description='Daily ETL of self-confidence quotes into Postgres and email notification',
    schedule_interval='@daily',  # runs once every day
    start_date=datetime(2025, 6, 24),  # start today
    catchup=False,
    tags=['etl', 'quotes', 'self_confidence'],
) as dag:

    def fetch_quote(**kwargs):
        url = "https://quotes-api12.p.rapidapi.com/quotes/random"
        headers = {
            "x-rapidapi-key": Variable.get("quotes_api_key"),
            "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
        }
        params = {"type": "selfconfidence"}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Push to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='quote_text', value=data['quote'])
        ti.xcom_push(key='quote_author', value=data['author'])
        ti.xcom_push(key='quote_type', value=data.get('type'))

    def insert_to_db(**kwargs):
        ti = kwargs['ti']
        quote = ti.xcom_pull(key='quote_text', task_ids='fetch_quote')
        author = ti.xcom_pull(key='quote_author', task_ids='fetch_quote')
        qtype = ti.xcom_pull(key='quote_type', task_ids='fetch_quote')

        conn = psycopg2.connect(
            dbname=Variable.get("db_name"),
            user=Variable.get("db_user"),
            password=Variable.get("db_password"),
            host=Variable.get("db_host"),
            port=Variable.get("db_port")
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS self_confidence (
                id SERIAL PRIMARY KEY,
                quote TEXT,
                author TEXT,
                type TEXT
            )
        """)

        cur.execute(
            """
            INSERT INTO self_confidence (quote, author, type)
            VALUES (%s, %s, %s)
            """,
            (quote, author, qtype)
        )

        conn.commit()
        cur.close()
        conn.close()

    def compose_email(**kwargs):
        ti = kwargs['ti']
        quote = ti.xcom_pull(key='quote_text', task_ids='fetch_quote')
        author = ti.xcom_pull(key='quote_author', task_ids='fetch_quote')
        return f"\"{quote}\" - {author}"

    fetch_task = PythonOperator(
        task_id='fetch_quote',
        python_callable=fetch_quote,
        provide_context=True,
    )

    insert_task = PythonOperator(
        task_id='insert_to_db',
        python_callable=insert_to_db,
        provide_context=True,
    )

    email_task = EmailOperator(
        task_id='send_email',
        to=EMAIL_RECIPIENTS,
        subject='Daily Self-Confidence Quote',
        html_content="{{ task_instance.xcom_pull(task_ids='fetch_quote', key='quote_text') }} - {{ task_instance.xcom_pull(task_ids='fetch_quote', key='quote_author') }}",
        smtp_host=EMAIL_CONFIG['smtp_host'],
        smtp_port=int(EMAIL_CONFIG['smtp_port']),
        smtp_user=EMAIL_CONFIG['smtp_user'],
        smtp_password=EMAIL_CONFIG['smtp_password'],
        smtp_mail_from=EMAIL_CONFIG['smtp_mail_from'],
    )

    fetch_task >> insert_task >> email_task
