# =========================================================
# IMPORT REQUIRED LIBRARIES
# =========================================================

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import pandas as pd
import psycopg2


# =========================================================
# CONFIGURATION VARIABLES
# =========================================================

# News API configuration
API_KEY = "9d64ba92867247f2a6c57a04a7eebc78"
BASE_URL = "https://newsapi.org/v2/top-headlines"

# PostgreSQL configuration
#DB_NAME = "YOUR_DB"
#DB_USER = "YOUR_USER"
#DB_PASSWORD = "YOUR_PASSWORD"
#DB_HOST = "localhost"
#DB_PORT = "5432"


# =========================================================
# DEFINE AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="news_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",   # run once per day
    catchup=False,
    tags=["ETL", "NewsAPI"]
) as dag:


    # =====================================================
    # EXTRACT TASK
    # Fetch data from NewsAPI
    # =====================================================

    @task
    def extract_news():
        """
        Calls the NewsAPI and retrieves technology news articles.
        Returns a list of JSON articles which will be pushed to XCom.
        """

        params = {
            "country": "us",
            "category": "technology",
            "apiKey": API_KEY
        }

        try:
            response = requests.get(BASE_URL, params=params)

            # Raise error if API call fails
            response.raise_for_status()

            data = response.json()

            # Extract articles list
            articles = data.get("articles", [])

            print(f"Extracted {len(articles)} articles")

            return articles   # returned value automatically stored in XCom

        except requests.exceptions.RequestException as e:
            print("API request failed:", e)
            return []


    # =====================================================
    # TRANSFORM TASK
    # Convert JSON → Clean DataFrame
    # =====================================================

    @task
    def transform_news(articles):
        """
        Cleans and converts JSON articles into a structured format.
        Returns cleaned records for loading into the database.
        """

        if not articles:
            return []

        # Convert JSON to DataFrame
        df = pd.DataFrame(articles)

        # Keep required columns
        df = df[["title", "source", "url"]].copy()

        # Extract source name from nested JSON
        df["source"] = df["source"].apply(
            lambda x: x.get("name") if isinstance(x, dict) else None
        )

        # Remove rows with missing values
        df.dropna(subset=["title", "source", "url"], inplace=True)

        # Remove duplicate articles
        df.drop_duplicates(subset=["url"], inplace=True)

        print(f"Clean records: {len(df)}")

        # Convert DataFrame → dictionary list (required for XCom)
        return df.to_dict("records")


    # =====================================================
    # LOAD TASK
    # Insert data into PostgreSQL
    # =====================================================

    @task
    def load_to_postgres(records):
        """
        Loads cleaned news records into PostgreSQL database.
        """

        if not records:
            print("No data to load.")
            return

        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )

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

            # Insert records
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
    # DAG PIPELINE (TASK DEPENDENCIES)
    # =====================================================

    # Extract → Transform → Load

    extracted_articles = extract_news()

    cleaned_articles = transform_news(extracted_articles)

    load_to_postgres(cleaned_articles)
