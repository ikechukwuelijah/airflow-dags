from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests

default_args = {
    'owner': 'Ike',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_price_dag',
    default_args=default_args,
    description='Fetch crypto prices and append to Postgres history table every hour',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['crypto', 'etl'],
) as dag:

    def fetch_crypto_prices(**kwargs):
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            'ids': 'bitcoin,ethereum,binancecoin',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true'
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        return [
            {'symbol': 'BTC', 'price_usd': data['bitcoin']['usd'],      'change_24h': data['bitcoin']['usd_24h_change']},
            {'symbol': 'ETH', 'price_usd': data['ethereum']['usd'],     'change_24h': data['ethereum']['usd_24h_change']},
            {'symbol': 'BNB', 'price_usd': data['binancecoin']['usd'],  'change_24h': data['binancecoin']['usd_24h_change']},
        ]

    def append_to_postgres(ti, **kwargs):
        crypto_data = ti.xcom_pull(task_ids='fetch_crypto_prices')
        if not crypto_data:
            raise ValueError("No data received from fetch_crypto_prices")

        hook = PostgresHook(postgres_conn_id='postgres_default')

        # Ensure history table exists with autoincrement id and timestamp
        hook.run("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id         SERIAL PRIMARY KEY,
                symbol     TEXT,
                price_usd  NUMERIC,
                change_24h NUMERIC,
                timestamp  TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # Insert each record to append history
        insert_sql = """
            INSERT INTO crypto_prices (symbol, price_usd, change_24h)
            VALUES (%s, %s, %s);
        """
        for rec in crypto_data:
            hook.run(insert_sql, parameters=(rec['symbol'], rec['price_usd'], rec['change_24h']))

    fetch_task = PythonOperator(
        task_id='fetch_crypto_prices',
        python_callable=fetch_crypto_prices,
    )

    insert_task = PythonOperator(
        task_id='append_to_postgres',
        python_callable=append_to_postgres,
    )

    fetch_task >> insert_task
