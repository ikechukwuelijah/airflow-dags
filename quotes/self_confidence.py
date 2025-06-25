#%%
import requests
import pandas as pd
import psycopg2

# API request
url = "https://quotes-api12.p.rapidapi.com/quotes/random"
querystring = {"type": "selfconfidence"}
headers = {
    "x-rapidapi-key": "",
    "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

# Convert to DataFrame
df = pd.DataFrame([data])  # ensure it's in list format

# Database connection credentials
conn = psycopg2.connect(
   
)

cur = conn.cursor()

# Create table if not exists
cur.execute("""
    CREATE TABLE IF NOT EXISTS self_confidence (
        id SERIAL PRIMARY KEY,
        quote TEXT,
        author TEXT,
        type TEXT
    )
""")

# Insert the data
cur.execute("""
    INSERT INTO self_confidence (quote, author, type)
    VALUES (%s, %s, %s)
""", (data['quote'], data['author'], data['type']))

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()

print("Quote inserted successfully into 'self_confidence' table.")

# %%
