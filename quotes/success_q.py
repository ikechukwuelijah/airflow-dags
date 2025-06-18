#%%
import requests
import psycopg2

def fetch_quote():
    url = "https://quotes-api12.p.rapidapi.com/quotes/random"
    querystring = {"type": "success"}
    headers = {
        "x-rapidapi-key": "",
        "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def load_to_postgres(data):
    conn = psycopg2.connect(
        
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS success (
            id SERIAL PRIMARY KEY,
            quote TEXT,
            author TEXT,
            type TEXT
        )
    """)

    cur.execute("""
        INSERT INTO success (quote, author, type)
        VALUES (%s, %s, %s)
    """, (data['quote'], data['author'], data['type']))

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("Fetching quote...")
    quote_data = fetch_quote()
    print("Quote fetched:", quote_data)

    print("Loading into PostgreSQL...")
    load_to_postgres(quote_data)
    print("Load complete!")

if __name__ == "__main__":
    main()

# %%
