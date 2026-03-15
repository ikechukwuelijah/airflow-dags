# =========================================================
# IMPORT REQUIRED LIBRARIES
# =========================================================

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import requests
import pandas as pd


# =========================================================
# CONFIGURATION VARIABLES
# =========================================================

# Retrieve API key securely from Airflow Variables
API_KEY = Variable.get("NEWS_API_KEY")

# NewsAPI endpoint
BASE_URL = "https://newsapi.org/v2/top-headlines"


# =========================================================
# DEFINE AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="news_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",      # DAG runs once per day
    catchup=False,                   # do not backfill previous runs
    tags=["ETL", "NewsAPI"]
) as dag:


    # =====================================================
    # EXTRACT TASK
    # Fetch technology news from NewsAPI
    # =====================================================

    @task
    def extract_news():
        """
        Calls NewsAPI and retrieves technology news articles.

        Returns:
            list: List of JSON article objects.
        """

        # API query parameters
        params = {
            "country": "us",
            "category": "technology",
            "apiKey": API_KEY
        }

        try:
            # Send GET request to API
            response = requests.get(BASE_URL, params=params)

            # Raise error if API request fails
            response.raise_for_status()

            # Convert response to JSON
            data = response.json()

            # Extract articles list
            articles = data.get("articles", [])

            print(f"Extracted {len(articles)} articles")

            # Returned data is automatically stored in XCom
            return articles

        except requests.exceptions.RequestException as e:
            print("API request failed:", e)
            return []


    # =====================================================
    # TRANSFORM TASK
    # Clean and structure the extracted JSON data
    # =====================================================

    @task
    def transform_news(articles):
        """
        Converts raw JSON articles into cleaned structured records.

        Steps:
        - Convert JSON to Pandas DataFrame
        - Select relevant columns
        - Extract source name from nested JSON
        - Remove null values
        - Remove duplicates

        Returns:
            list: Cleaned records as dictionaries
        """

        if not articles:
            print("No articles received from extract step")
            return []

        # Convert JSON list to DataFrame
        df = pd.DataFrame(articles)

        # Select required columns
        df = df[["title", "source", "url"]].copy()

        # Extract source name from nested dictionary
        df["source"] = df["source"].apply(
            lambda x: x.get("name") if isinstance(x, dict) else None
        )

        # Remove rows with missing important fields
        df.dropna(subset=["title", "source", "url"], inplace=True)

        # Remove duplicate articles based on URL
        df.drop_duplicates(subset=["url"], inplace=True)

        print(f"Clean records: {len(df)}")

        # Convert DataFrame → list of dictionaries for XCom
        return df.to_dict("records")


    # =====================================================
    # LOAD TASK
    # Insert cleaned data into PostgreSQL
    # =====================================================

    @task
    def load_to_postgres(records):
        """
        Loads cleaned records into PostgreSQL.

        Uses Airflow PostgresHook to securely retrieve
        database credentials from Airflow Connections.
        """

        if not records:
            print("No data to load.")
            return

        try:
            # Create Postgres connection using Airflow Connection ID
            hook = PostgresHook(postgres_conn_id="postgres_news")

            # Get psycopg2 connection from hook
            conn = hook.get_conn()
            cursor = conn.cursor()

            print("Connected to PostgreSQL")

            # Create table if it does not exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS tech_news (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL
            );
            """)

            conn.commit()

            # Insert records into table
            for record in records:

                cursor.execute("""
                INSERT INTO tech_news (title, source, url)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
                """, (
                    record["title"],
                    record["source"],
                    record["url"]
                ))

            conn.commit()

            print(f"{len(records)} records inserted")

        except Exception as e:
            print("Database error:", e)

        finally:

            cursor.close()
            conn.close()

            print("PostgreSQL connection closed")


    # =====================================================
    # DAG TASK DEPENDENCIES
    # =====================================================

    # Task 1: Extract news articles from API
    extracted_articles = extract_news()

    # Task 2: Clean and transform extracted data
    cleaned_articles = transform_news(extracted_articles)

    # Task 3: Load cleaned data into PostgreSQL
    load_to_postgres(cleaned_articles)
