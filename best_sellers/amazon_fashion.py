
# %% step 3
import requests
import json
import pandas as pd
import psycopg2
from psycopg2 import sql

url = "https://real-time-amazon-data.p.rapidapi.com/search"

querystring = {"query":"Fashion","page":"1","country":"CA","sort_by":"BEST_SELLERS","category_id":"LOWEST_PRICE","product_condition":"ALL","is_prime":"false","deals_and_discounts":"NONE"}

headers = {
    "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

data = response.json()
if 'data' in data and 'products' in data['data']:
    df = pd.DataFrame(data['data']['products'])
    print(df.head())  # Display the first few rows
else:
    print("No product data found in the response.")

# PostgreSQL connection details
conn = psycopg2.connect(
    host="127.0.0.1",
    database="dwh",
    user="postgres",
    password="DataEngineer001",
    port="5432"
)

cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS amazon_products (
    asin TEXT PRIMARY KEY,
    product_title TEXT,
    product_price TEXT,
    product_original_price TEXT,
    currency TEXT,
    country TEXT,
    product_star_rating FLOAT,
    product_num_ratings BIGINT,
    product_url TEXT,
    product_photo TEXT,
    product_num_offers BIGINT,
    product_minimum_offer_price TEXT,
    is_best_seller BOOLEAN,
    is_amazon_choice BOOLEAN,
    is_prime BOOLEAN,
    climate_pledge_friendly BOOLEAN,
    sales_volume TEXT,
    delivery TEXT
)
"""
cur.execute(create_table_query)
conn.commit()

if 'data' in data and 'products' in data['data']:
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO amazon_products (
            asin,
            product_title,
            product_price,
            product_original_price,
            currency,
            country,
            product_star_rating,
            product_num_ratings,
            product_url,
            product_photo,
            product_num_offers,
            product_minimum_offer_price,
            is_best_seller,
            is_amazon_choice,
            is_prime,
            climate_pledge_friendly,
            sales_volume,
            delivery
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin) DO UPDATE SET
            product_title = EXCLUDED.product_title,
            product_price = EXCLUDED.product_price,
            product_original_price = EXCLUDED.product_original_price,
            currency = EXCLUDED.currency,
            country = EXCLUDED.country,
            product_star_rating = EXCLUDED.product_star_rating,
            product_num_ratings = EXCLUDED.product_num_ratings,
            product_url = EXCLUDED.product_url,
            product_photo = EXCLUDED.product_photo,
            product_num_offers = EXCLUDED.product_num_offers,
            product_minimum_offer_price = EXCLUDED.product_minimum_offer_price,
            is_best_seller = EXCLUDED.is_best_seller,
            is_amazon_choice = EXCLUDED.is_amazon_choice,
            is_prime = EXCLUDED.is_prime,
            climate_pledge_friendly = EXCLUDED.climate_pledge_friendly,
            sales_volume = EXCLUDED.sales_volume,
            delivery = EXCLUDED.delivery
        """

        cur.execute(insert_query, (
            row.get('asin'),
            row.get('product_title'),
            row.get('product_price'),
            row.get('product_original_price'),
            row.get('currency'),
            row.get('country'),
            float(row['product_star_rating']) if pd.notnull(row.get('product_star_rating')) else None,
            int(row['product_num_ratings']) if pd.notnull(row.get('product_num_ratings')) else None,
            row.get('product_url'),
            row.get('product_photo'),
            int(row['product_num_offers']) if pd.notnull(row.get('product_num_offers')) else None,
            row.get('product_minimum_offer_price'),
            bool(row.get('is_best_seller')) if pd.notnull(row.get('is_best_seller')) else None,
            bool(row.get('is_amazon_choice')) if pd.notnull(row.get('is_amazon_choice')) else None,
            bool(row.get('is_prime')) if pd.notnull(row.get('is_prime')) else None,
            bool(row.get('climate_pledge_friendly')) if pd.notnull(row.get('climate_pledge_friendly')) else None,
            row.get('sales_volume'),
            row.get('delivery')
        ))

    conn.commit()
    print("Data loaded successfully into PostgreSQL.")

cur.close()
conn.close()
# %%
