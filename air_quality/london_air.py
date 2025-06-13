from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests
import pandas as pd
import smtplib
from email.message import EmailMessage

# Default arguments applied to all tasks
default_args = {
    'owner': 'Ike',        
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Define the DAG
with DAG(
    dag_id='london_air_quality_etl',
    default_args=default_args,
    description='Daily ETL for air quality data',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 13),
    catchup=False,
    tags=['air_quality', 'etl'],
) as dag:

    def extract(**context):
        """
        Extracts yesterday's air pollution history from OpenWeatherMap API.
        Pushes raw data to XCom for downstream tasks.
        """
        api_key = Variable.get('OPENWEATHER_API_KEY', default_var=None)
        if not api_key:
            raise ValueError("OPENWEATHER_API_KEY is not set in Airflow Variables.")

        lat = 51.5074
        lon = -0.1278
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        start_of_yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_yesterday = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

        url = 'http://api.openweathermap.org/data/2.5/air_pollution/history'
        params = {
            'lat': lat,
            'lon': lon,
            'start': int(start_of_yesterday.timestamp()),
            'end': int(end_of_yesterday.timestamp()),
            'appid': api_key
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_list = response.json().get('list', [])
        # Push both the raw data and metadata to XCom
        context['ti'].xcom_push(key='raw_data', value=raw_list)
        context['ti'].xcom_push(key='window', value={
            'start': start_of_yesterday.isoformat(),
            'end': end_of_yesterday.isoformat()
        })

    def transform(**context):
        """
        Transforms raw JSON entries into list of dictionaries suitable for DB loading.
        Pushes transformed records to XCom.
        """
        ti = context['ti']
        raw_list = ti.xcom_pull(key='raw_data', task_ids='extract') or []
        CITY_NAME = 'London'
        LOCATION_NAME = 'London Central AQ'
        rows = []
        for entry in raw_list:
            row = {
                'timestamp': datetime.utcfromtimestamp(entry['dt']),
                'city': CITY_NAME,
                'location': LOCATION_NAME,
                'aqi': entry['main']['aqi'],
            }
            # flatten components
            row.update({k if k != 'pm2_5' else 'pm25': v for k, v in entry['components'].items()})
            rows.append(row)
        df = pd.DataFrame(rows)
        df.rename(columns={'pm2_5': 'pm25'}, inplace=True)
        # Push transformed records (as dicts) to XCom
        context['ti'].xcom_push(key='transformed', value=df.to_dict(orient='records'))

    def load_raw(**context):
        """
        Loads transformed records into raw_air_quality table in Postgres.
        """
        ti = context['ti']
        records = ti.xcom_pull(key='transformed', task_ids='transform') or []
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cur = conn.cursor()
        # Create raw table if missing
        cur.execute('''
            CREATE TABLE IF NOT EXISTS raw_air_quality (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                city TEXT,
                location TEXT,
                aqi INTEGER,
                co FLOAT, no FLOAT, no2 FLOAT,
                o3 FLOAT, so2 FLOAT,
                pm25 FLOAT, pm10 FLOAT, nh3 FLOAT
            );
        ''')
        conn.commit()
        # Insert rows
        for r in records:
            cur.execute('''
                INSERT INTO raw_air_quality
                (timestamp, city, location, aqi, co, no, no2, o3, so2, pm25, pm10, nh3)
                VALUES (%(timestamp)s, %(city)s, %(location)s, %(aqi)s,
                        %(co)s, %(no)s, %(no2)s, %(o3)s,
                        %(so2)s, %(pm25)s, %(pm10)s, %(nh3)s)
                ON CONFLICT DO NOTHING;
            ''', r)
        conn.commit()
        cur.close()
        conn.close()

    def load_staging(**context):
        """
        Transforms raw data into staging table stg_air_quality.
        """
        hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = '''
            CREATE TABLE IF NOT EXISTS stg_air_quality (
                city TEXT,
                location TEXT,
                parameter TEXT,
                clean_value NUMERIC(10,2),
                unit TEXT,
                measurement_date DATE
            );
            INSERT INTO stg_air_quality (city, location, parameter, clean_value, unit, measurement_date)
            SELECT city, location, 'pm25', ROUND(pm25::NUMERIC,2), 'µg/m³', timestamp::DATE FROM raw_air_quality WHERE pm25 IS NOT NULL
            UNION ALL
            SELECT city, location, 'pm10', ROUND(pm10::NUMERIC,2), 'µg/m³', timestamp::DATE FROM raw_air_quality WHERE pm10 IS NOT NULL;
        '''
        hook.run(sql)

    def load_dw(**context):
        """
        Creates dimension/fact tables and populates them from staging.
        """
        hook = PostgresHook(postgres_conn_id='postgres_default')
        # Create dimension table
        hook.run('''
            CREATE TABLE IF NOT EXISTS dim_location_air_quality (
                location_id SERIAL PRIMARY KEY,
                city TEXT,
                location TEXT
            );
        ''')
        # Create fact table
        hook.run('''
            CREATE TABLE IF NOT EXISTS fact_air_quality (
                location_id INT REFERENCES dim_location_air_quality(location_id),
                parameter TEXT,
                measurement_date DATE,
                avg_value DOUBLE PRECISION
            );
        ''')
        # Load dimension
        hook.run('''
            INSERT INTO dim_location_air_quality (city, location)
            SELECT DISTINCT city, location FROM stg_air_quality
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_location_air_quality d
                WHERE d.city = stg_air_quality.city
                  AND d.location = stg_air_quality.location
            );
        ''')
        # Load fact
        hook.run('''
            INSERT INTO fact_air_quality (location_id, parameter, measurement_date, avg_value)
            SELECT d.location_id, s.parameter, s.measurement_date, AVG(s.clean_value)
            FROM stg_air_quality s
            JOIN dim_location_air_quality d
              ON s.city = d.city AND s.location = d.location
            GROUP BY d.location_id, s.parameter, s.measurement_date;
        ''')

    def data_quality(**context):
        """
        Performs data quality check and sends email if record count below threshold.
        Skips email alert if email variables are not configured.
        """
        EXPECTED_MIN = 10
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM fact_air_quality;')
        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        if count < EXPECTED_MIN:
            # Check if email settings are configured
            email_sender   = Variable.get('EMAIL_SENDER', default_var=None)
            email_password = Variable.get('EMAIL_PASSWORD', default_var=None)
            email_receiver = Variable.get('EMAIL_RECEIVER', default_var=None)

            if not all([email_sender, email_password, email_receiver]):
                print("⚠️ Email settings not configured. Skipping alert email.")
            else:
                # build and send email
                smtp = smtplib.SMTP('smtp.gmail.com', 587)
                smtp.starttls()
                smtp.login(email_sender, email_password)
                msg = EmailMessage()
                msg['Subject'] = f"🚨 Low Data Volume: {count} records"
                msg['From'] = email_sender
                msg['To'] = email_receiver
                msg.set_content(f"Only {count} records in fact_air_quality, expected at least {EXPECTED_MIN}.")
                smtp.send_message(msg)
                smtp.quit()
        else:
            print(f"✅ Data quality check passed with {count} records.")

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    load_raw_task = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw
    )

    load_staging_task = PythonOperator(
        task_id='load_staging',
        python_callable=load_staging
    )

    load_dw_task = PythonOperator(
        task_id='load_dw',
        python_callable=load_dw
    )

    dq_task = PythonOperator(
        task_id='data_quality',
        python_callable=data_quality
    )

    # Set task dependencies
    extract_task >> transform_task >> load_raw_task >> load_staging_task >> load_dw_task >> dq_task
