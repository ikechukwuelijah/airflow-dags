from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

# Task 1: Fetch quote from API
def fetch_quote(**kwargs):
    url = "https://quotes-api12.p.rapidapi.com/quotes/random"
    querystring = {"type": "selfconfidence"}
    headers = {
        "x-rapidapi-key": "",
        "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    quote_data = response.json()
    kwargs['ti'].xcom_push(key='quote_data', value=quote_data)


# Task 2: Send plain text email
def send_quote_as_text_email(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    email_config = Variable.get("email_config", deserialize_json=True)
    smtp_host     = email_config['smtp_host']
    smtp_port     = email_config['smtp_port']
    smtp_user     = email_config['smtp_user']
    smtp_password = email_config['smtp_password']
    sender_email  = email_config['sender_email']
    receiver_email= email_config['receiver_email']

    quote  = data.get('quote', 'No quote found.')
    author = data.get('author', 'Unknown')
    qtype  = data.get('type', 'N/A')
    body   = f"Here is your daily self-confidence quote:\n\n\"{quote}\"\n\n- {author} ({qtype})"

    msg = MIMEMultipart()
    msg['Subject'] = 'Daily Self-Confidence Quote'
    msg['From']    = sender_email
    msg['To']      = ", ".join(receiver_email)
    msg.attach(MIMEText(body, 'plain'))

    try:
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()

        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("Text email sent successfully")
    except Exception as e:
        print(f"Text email sending failed: {e}")
        raise


# Task 3: Load quote into PostgreSQL
def load_to_postgres(**kwargs):
    ti   = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

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
    """)

    cur.execute("""
        INSERT INTO self_confidence (quote, author, type)
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

# Set task dependencies
fetch_task >> [text_email_task, load_task]
