from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Default arguments for the DAG
default_args = {
    'owner': 'Ike',
    'start_date': datetime(2025, 6, 18),
    'retries': 1,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    dag_id='self_confidence_dag',
    default_args=default_args,
    description='Daily ETL: fetch self-confidence quote, send text email, and load to Postgres',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'quotes', 'self_confidence'],
)


def fetch_quote(**kwargs):
    """
    Task 1: Fetch quote from API and push to XCom
    """
    api_key = Variable.get('quotes_api_key', default_var=None)
    if not api_key:
        raise ValueError("Airflow Variable 'quotes_api_key' is missing. Please set it to your RapidAPI key.")

    url = "https://quotes-api12.p.rapidapi.com/quotes/random"
    params = {"type": "selfconfidence"}
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=params)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        # Log for debugging before failing
        print(f"Quote API returned {response.status_code}: {response.text}")
        raise

    data = response.json()
    ti = kwargs['ti']
    ti.xcom_push(key='quote_data', value=data)


def send_quote_as_text_email(**kwargs):
    """
    Task 2: Send quote as plain text email to multiple recipients
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    email_config = Variable.get("email_config", default_var={}, deserialize_json=True)
    if not email_config:
        raise ValueError("Airflow Variable 'email_config' is missing or empty.")

    smtp_host       = email_config.get('smtp_host')
    smtp_port       = int(email_config.get('smtp_port', 587))
    smtp_user       = email_config.get('smtp_user')
    smtp_password   = email_config.get('smtp_password')
    sender_email    = email_config.get('sender_email')
    receiver_emails = email_config.get('receiver_email', [])

    if not all([smtp_host, smtp_user, smtp_password, sender_email, receiver_emails]):
        raise ValueError("Incomplete email_config: ensure smtp_host, smtp_user, smtp_password, sender_email, and receiver_email are set.")

    quote = data.get('quote') or data.get('text') or 'No quote found.'
    author = data.get('author', 'Unknown')
    qtype = data.get('type') or data.get('category') or 'N/A'

    body = f"Here is your daily self-confidence quote:\n\n\"{quote}\"\n\n- {author} ({qtype})"

    msg = MIMEMultipart()
    msg['Subject'] = 'Daily Self-Confidence Quote'
    msg['From']    = sender_email
    msg['To']      = ", ".join(receiver_emails)
    msg.attach(MIMEText(body, 'plain'))

    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()

    server.login(smtp_user, smtp_password)
    server.sendmail(sender_email, receiver_emails, msg.as_string())
    server.quit()
    print("Text email sent successfully")


def load_to_postgres(**kwargs):
    """
    Task 3: Load the quote record into Postgres
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    # Normalize data structure
    if isinstance(data, list) and data:
        record = data[0]
    elif isinstance(data, dict) and 'data' in data and isinstance(data['data'], list) and data['data']:
        record = data['data'][0]
    elif isinstance(data, dict):
        record = data
    else:
        raise RuntimeError(f"Unexpected XCom data format: {data!r}")

    quote_text = record.get('quote') or record.get('text') or 'No quote'
    author     = record.get('author', 'Unknown')
    qtype      = record.get('type') or record.get('category') or 'N/A'

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS self_confidence (
            id SERIAL PRIMARY KEY,
            quote TEXT,
            author TEXT,
            type TEXT
        )
    """
    )
    cur.execute(
        "INSERT INTO self_confidence (quote, author, type) VALUES (%s, %s, %s)",
        (quote_text, author, qtype)
    )
    conn.commit()
    cur.close()
    conn.close()

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_quote',
    python_callable=fetch_quote,
    provide_context=True,
    dag=dag
)

email_task = PythonOperator(
    task_id='send_text_email',
    python_callable=send_quote_as_text_email,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_quote',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

# Set task dependencies
fetch_task >> [email_task, load_task]
