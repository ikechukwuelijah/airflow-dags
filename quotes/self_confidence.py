from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import ast

# Default arguments for the DAG
default_args = {
    'owner': 'Ike',
    'start_date': datetime(2025, 6, 18),
    'retries': 1
}

dag = DAG(
    'self_confidence_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Fetch a random self-confidence quote

def fetch_quote(**kwargs):
    url = "https://quotes-api12.p.rapidapi.com/quotes/random"
    querystring = {"type": "selfconfidence"}
    headers = {
        "x-rapidapi-key": "<YOUR_RAPIDAPI_KEY>",
        "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    quote_data = response.json()
    # Push to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='quote_data', value=quote_data)

# Task 2: Send the quote as a plain-text email
def send_quote_as_text_email(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    if not data or not all(k in data for k in ('quote', 'author', 'type')):
        raise RuntimeError(f"Invalid quote payload: {data!r}")

    # Safely load email_config variable (try JSON first, then Python literal)
    raw = Variable.get("email_config")
    try:
        email_config = json.loads(raw)
    except json.JSONDecodeError:
        email_config = ast.literal_eval(raw)

    smtp_host      = email_config['smtp_host']
    smtp_port      = email_config['smtp_port']
    smtp_user      = email_config['smtp_user']
    smtp_password  = email_config['smtp_password']
    sender_email   = email_config['sender_email']
    receiver_emails= email_config['receiver_email']

    quote  = data['quote']
    author = data['author']
    qtype  = data['type']
    body   = f"Here is your daily self-confidence quote:\n\n\"{quote}\"\n\n- {author} ({qtype})"

    msg = MIMEMultipart()
    msg['Subject'] = 'Daily Self-Confidence Quote'
    msg['From']    = sender_email
    msg['To']      = ", ".join(receiver_emails)
    msg.attach(MIMEText(body, 'plain'))

    try:
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()

        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, receiver_emails, msg.as_string())
        server.quit()
        print("Text email sent successfully")
    except Exception as e:
        print(f"Text email sending failed: {e}")
        raise

# Task 3: Load quote into PostgreSQL
def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    if not data or not all(k in data for k in ('quote', 'author', 'type')):
        raise RuntimeError(f"Invalid quote payload: {data!r}")

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur  = conn.cursor()

    # Create table if not exists, renaming 'type' column to avoid reserved keyword
    cur.execute("""
        CREATE TABLE IF NOT EXISTS self_confidence (
            id SERIAL PRIMARY KEY,
            quote TEXT,
            author TEXT,
            quote_type TEXT
        )
    """)

    # Insert using quote_type column
    cur.execute("""
        INSERT INTO self_confidence (quote, author, quote_type)
        VALUES (%s, %s, %s)
    """, (data['quote'], data['author'], data['type']))

    conn.commit()
    cur.close()
    conn.close()

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_quote',
    python_callable=fetch_quote,
    dag=dag
)

text_email_task = PythonOperator(
    task_id='send_text_email',
    python_callable=send_quote_as_text_email,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_quote',
    python_callable=load_to_postgres,
    dag=dag
)

# Set dependencies
fetch_task >> [text_email_task, load_task]
