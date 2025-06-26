#%%
import requests
import psycopg2
from psycopg2 import sql

# Configuration
DB_CONFIG = {
    '              # Default PostgreSQL port
}

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_PARAMS = {
    'ids': 'bitcoin,ethereum,binancecoin',
    'vs_currencies': 'usd',
    'include_24hr_change': 'true'
}

CREATE_TABLE_SQL = sql.SQL("""
    CREATE TABLE IF NOT EXISTS crypto_prices (
        symbol TEXT PRIMARY KEY,
        price_usd NUMERIC,
        change_24h NUMERIC,
        last_updated TIMESTAMP DEFAULT NOW()
    )
""")

INSERT_SQL = sql.SQL("""
    INSERT INTO crypto_prices (symbol, price_usd, change_24h)
    VALUES (%s, %s, %s)
    ON CONFLICT (symbol) DO UPDATE
      SET price_usd = EXCLUDED.price_usd,
          change_24h = EXCLUDED.change_24h,
          last_updated = NOW();
""")


def fetch_crypto_prices():
    """
    Fetches cryptocurrency prices from the CoinGecko API.
    Returns:
        List[dict]: A list of records with keys 'symbol', 'price_usd', 'change_24h'.
    """
    response = requests.get(COINGECKO_URL, params=COINGECKO_PARAMS)
    response.raise_for_status()
    data = response.json()
    return [
        {
            'symbol': 'BTC',
            'price_usd': data['bitcoin']['usd'],
            'change_24h': data['bitcoin']['usd_24h_change']
        },
        {
            'symbol': 'ETH',
            'price_usd': data['ethereum']['usd'],
            'change_24h': data['ethereum']['usd_24h_change']
        },
        {
            'symbol': 'BNB',
            'price_usd': data['binancecoin']['usd'],
            'change_24h': data['binancecoin']['usd_24h_change']
        }
    ]


def load_into_postgres(records):
    """
    Inserts or updates cryptocurrency price records into Postgres.
    Args:
        records (List[dict]): List of price records.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute(CREATE_TABLE_SQL)

    # Upsert each record
    for rec in records:
        cur.execute(INSERT_SQL, (rec['symbol'], rec['price_usd'], rec['change_24h']))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    print("Fetching cryptocurrency prices...")
    prices = fetch_crypto_prices()
    print(f"Fetched data for: {', '.join(r['symbol'] for r in prices)}")

    print("Loading data into Postgres...")
    load_into_postgres(prices)
    print("ETL job completed successfully.")

# %%
