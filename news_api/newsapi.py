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


# =========================================================
# DEFAULT DAG SETTINGS
# =========================================================

default_args = {
    'owner': 'Ike',
    'start_date': datetime(2025, 6, 18),
    'retries': 1,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


# =========================================================
# DEFINE DAG
# =========================================================

dag = DAG(
    dag_id='tech_news_email_pipeline',
    default_args=default_args,
    description='Daily ETL: fetch tech news, send email, and load to Postgres',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'news', 'email']
)


# =========================================================
# TASK 1: FETCH NEWS FROM API
# =========================================================

def fetch_news(**kwargs):

    api_key = Variable.get("NEWS_API_KEY")

    url = "https://newsapi.org/v2/top-headlines"

    params = {
        "country": "us",
        "category": "technology",
        "apiKey": api_key
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    articles = data.get("articles", [])

    print(f"Fetched {len(articles)} articles")

    ti = kwargs['ti']
    ti.xcom_push(key="news_data", value=articles)


# =========================================================
# TASK 2: SEND NEWS EMAIL
# =========================================================

def send_news_email(**kwargs):

    ti = kwargs['ti']
    articles = ti.xcom_pull(task_ids="fetch_news", key="news_data")

    if not articles:
        print("No news available to email")
        return

    # get email configuration from Airflow Variables
    email_config = Variable.get(
        "email_config",
        default_var={},
        deserialize_json=True
    )

    smtp_host = email_config.get("smtp_host")
    smtp_port = int(email_config.get("smtp_port", 587))
    smtp_user = email_config.get("smtp_user")
    smtp_password = email_config.get("smtp_password")

    sender_email = email_config.get("sender_email")
    receiver_emails = email_config.get("receiver_email", [])

    # build email content
    body = "Today's Top Technology News\n\n"

    for article in articles[:10]:

        title = article.get("title")
        source = article.get("source", {}).get("name")
        url = article.get("url")

        body += f"{title}\nSource: {source}\n{url}\n\n"

    msg = MIMEMultipart()

    msg["Subject"] = "Daily Technology News"
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_emails)

    msg.attach(MIMEText(body, "plain"))

    # send email
    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()

    server.login(smtp_user, smtp_password)

    server.sendmail(sender_email, receiver_emails, msg.as_string())

    server.quit()

    print("Tech news email sent successfully")


# =========================================================
# TASK 3: TRANSFORM + LOAD INTO POSTGRES
# =========================================================

def load_news_to_postgres(**kwargs):

    ti = kwargs['ti']

    articles = ti.xcom_pull(task_ids="fetch_news", key="news_data")

    if not articles:
        print("No data to load")
        return

    df = pd.DataFrame(articles)

    df = df[["title", "source", "url"]].copy()

    df["source"] = df["source"].apply(
        lambda x: x.get("name") if isinstance(x, dict) else None
    )

    df.dropna(subset=["title", "source", "url"], inplace=True)

    df.drop_duplicates(subset=["url"], inplace=True)

    hook = PostgresHook(postgres_conn_id="postgres_news")

    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tech_news (
            id SERIAL PRIMARY KEY,
            title TEXT,
            source TEXT,
            url TEXT UNIQUE
        )
    """)

    for _, row in df.iterrows():

        cursor.execute("""
            INSERT INTO tech_news (title, source, url)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """, (row["title"], row["source"], row["url"]))

    conn.commit()

    cursor.close()
    conn.close()

    print(f"{len(df)} records processed")


# =========================================================
# DEFINE TASKS
# =========================================================

fetch_task = PythonOperator(
    task_id="fetch_news",
    python_callable=fetch_news,
    provide_context=True,
    dag=dag
)

email_task = PythonOperator(
    task_id="send_news_email",
    python_callable=send_news_email,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_news",
    python_callable=load_news_to_postgres,
    provide_context=True,
    dag=dag
)


# =========================================================
# TASK DEPENDENCIES
# =========================================================

fetch_task >> [email_task, load_task]
