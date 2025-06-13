#%% 
import requests
import pandas as pd
import psycopg2

# ========== CONFIGURATION ==========
API_KEY     = ""  # your NewsAPI key
DB_NAME     = ""
DB_USER     = ""
DB_PASSWORD = ""
DB_HOST     = ""   # force TCP to Windows Postgres
DB_PORT     = "5432"

# ========== EXTRACT ==========
def fetch_news(api_key):
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "country": "us",
        "category": "technology",
        "apiKey": api_key
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("articles", [])

# ========== TRANSFORM ==========
def transform_to_dataframe(articles):
    if not articles:
        return pd.DataFrame(columns=["title", "source", "url"])
    df = pd.DataFrame(articles)
    df = df[["title", "source", "url"]].copy()
    df["source"] = df["source"].apply(
        lambda x: x.get("name") if isinstance(x, dict) else str(x)
    )
    df.dropna(subset=["title", "source", "url"], inplace=True)
    df.drop_duplicates(subset=["url"], inplace=True)
    return df

# ========== LOAD ==========
def load_to_postgres(df):
    if df.empty:
        print("⚠️ No data to load.")
        return

    print("🔌 Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        # debug: show which host/socket we're actually using
        print("🔍 DSN parameters:", conn.get_dsn_parameters())

        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tech_news (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL
            );
        """)
        conn.commit()
        print("🛠️ Table check/created.")

        inserted = 0
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO tech_news (title, source, url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (url) DO NOTHING;
                """, (row["title"], row["source"], row["url"]))
                inserted += 1
            except Exception as e:
                print(f"❌ Insert error: {e}")
                conn.rollback()
            else:
                conn.commit()

        print(f"✅ {inserted} rows inserted.")

    except Exception as e:
        print("❌ Connection or query error:", e)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# ========== MAIN ==========
def main():
    print("📡 Fetching news from API...")
    articles = fetch_news(API_KEY)

    print("🧼 Transforming data into DataFrame...")
    df = transform_to_dataframe(articles)

    print("\n🧾 Preview of News Data:")
    print(df.head(10))
    print(f"\n🔢 Total articles: {len(df)}")

    # automatically load without waiting for input
    print("⬆️ Loading data into PostgreSQL…")
    load_to_postgres(df)

if __name__ == "__main__":
    main()

# %%
