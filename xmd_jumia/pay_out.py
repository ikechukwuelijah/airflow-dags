from datetime import datetime
from pathlib import Path
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from psycopg2.extras import execute_values

# ─── Configuration ─────────────────────────────────────────────────────────────
EXPORT_DIR = Path("/mnt/c/DEprojects/xmd_jumia/pay_out")
TABLE_NAME = "pay_out"

# Default DAG args
default_args = {
    'owner': 'Ike',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='pay_out_etl',
    default_args=default_args,
    description='Weekly ETL for pay_out CSV into Postgres',
    schedule_interval='0 0 * * 2',  # every Tuesday at midnight
    start_date=datetime(2025, 6, 17),  # a Tuesday
    catchup=False,
    tags=['etl', 'weekly', 'pay_out', 'jumia', 'xmd'],
) as dag:

    def fetch_and_normalize(**kwargs):
        """
        Finds the latest export CSV, loads into DataFrame, normalizes columns,
        and pushes records to XCom.
        """
        if not EXPORT_DIR.exists():
            raise FileNotFoundError(f"Export directory not found: {EXPORT_DIR}")
        files = list(EXPORT_DIR.glob('export-*.csv'))
        if not files:
            raise FileNotFoundError(f"No files matching 'export-*.csv' in {EXPORT_DIR}")
        latest = max(files, key=lambda p: p.stat().st_mtime)
        df = pd.read_csv(latest)
        df.columns = [c.strip().lower().replace(' ', '_').replace('.', '') for c in df.columns]
        # push list of record dicts
        kwargs['ti'].xcom_push(key='records', value=df.to_dict(orient='records'))

    def load_to_postgres(**kwargs):
        """
        Pulls records from XCom, ensures target table and columns, and bulk-inserts data.
        """
        records = kwargs['ti'].xcom_pull(task_ids='fetch_and_normalize', key='records')
        if not records:
            raise ValueError('No records found in XCom')
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cur = conn.cursor()
        # create base table
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """
        ).format(table=sql.Identifier(TABLE_NAME)))
        # existing cols
        cur.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name = %s
        """, [TABLE_NAME])
        existing = {r[0] for r in cur.fetchall()}
        # add any new columns
        columns = records[0].keys()
        for col in columns:
            if col not in existing:
                cur.execute(sql.SQL(
                    "ALTER TABLE {table} ADD COLUMN {col} TEXT;"
                ).format(table=sql.Identifier(TABLE_NAME), col=sql.Identifier(col)))
        # bulk insert
        cols = list(columns)
        insert_stmt = sql.SQL('INSERT INTO {table} ({fields}) VALUES %s').format(
            table=sql.Identifier(TABLE_NAME),
            fields=sql.SQL(', ').join(map(sql.Identifier, cols))
        )
        values = [tuple(str(rec.get(c)) if rec.get(c) is not None else None for c in cols) for rec in records]
        execute_values(cur, insert_stmt, values)
        conn.commit()
        cur.close()
        conn.close()

    fetch_task = PythonOperator(
        task_id='fetch_and_normalize',
        python_callable=fetch_and_normalize,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    fetch_task >> load_task
