#%% 

# ==============================
# IMPORT REQUIRED LIBRARIES
# ==============================
import requests
import pandas as pd
import psycopg2


# ==============================
# API CONFIGURATION
# ==============================

# Store API key in a variable (avoid hardcoding it multiple times)
API_KEY = ""

# NewsAPI endpoint
BASE_URL = "https://newsapi.org/v2/top-headlines"


# ==============================
# EXTRACT FUNCTION
# Fetch data from the API
# ==============================

def fetch_news(api_key):
    """
    Fetch technology news articles from NewsAPI.
    Returns a list of article JSON objects.
    """

    # Query parameters sent to the API
    params = {
        "country": "us",           # get US news
        "category": "technology",  # filter for technology news
        "apiKey": api_key          # authentication key
    }

    try:
        # Send GET request to API
        response = requests.get(BASE_URL, params=params)

        # Raise error if request fails (status codes like 404, 401, etc.)
        response.raise_for_status()

        # Convert response into JSON
        data = response.json()

        # Extract only the articles section
        articles = data.get("articles", [])

        return articles

    except requests.exceptions.RequestException as e:
        print("API request failed:", e)
        return []


# ==============================
# TRANSFORM FUNCTION
# Convert JSON → Clean DataFrame
# ==============================

def transform_to_dataframe(articles):
    """
    Convert raw article JSON data into a clean Pandas DataFrame.
    """

    # If API returned no data, return empty dataframe
    if not articles:
        return pd.DataFrame(columns=["title", "source", "url"])

    # Convert JSON list into DataFrame
    df = pd.DataFrame(articles)

    # Keep only required columns
    df = df[["title", "source", "url"]].copy()

    # Extract source name from nested JSON structure
    # Example: {"id": None, "name": "TechCrunch"} → "TechCrunch"
    df["source"] = df["source"].apply(
        lambda x: x.get("name") if isinstance(x, dict) else None
    )

    # Remove rows where important fields are missing
    df.dropna(subset=["title", "source", "url"], inplace=True)

    # Remove duplicate news articles using the URL
    df.drop_duplicates(subset=["url"], inplace=True)

    return df


# ==============================
# MAIN PIPELINE
# ==============================

# Step 1: Extract data from API
articles = fetch_news(API_KEY)

print("Total Articles Retrieved:", len(articles))

# Step 2: Transform JSON → DataFrame
news_df = transform_to_dataframe(articles)

# Step 3: Preview cleaned dataset
print("\nCleaned News Data:")
print(news_df.head())

print("\nTotal Clean Records:", len(news_df))

# ==============================
# DATABASE CONFIGURATION
# ==============================

DB_NAME = ""
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"


# ==============================
# LOAD FUNCTION
# Insert DataFrame into Postgres
# ==============================

def load_to_postgres(df):
    """
    Load cleaned news DataFrame into PostgreSQL table.
    """

    # If dataframe is empty, skip loading
    if df.empty:
        print("No data to load.")
        return

    try:
        # Create connection to PostgreSQL
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

        print("Table ready")

        # Insert records from DataFrame
        for _, row in df.iterrows():

            cursor.execute("""
            INSERT INTO tech_news (title, source, url)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
            """, (row["title"], row["source"], row["url"]))

        conn.commit()

        print(f"{len(df)} rows loaded successfully")

    except Exception as e:
        print("Database error:", e)

    finally:
        # Close database connection
        cursor.close()
        conn.close()

        print("PostgreSQL connection closed")
# %%
