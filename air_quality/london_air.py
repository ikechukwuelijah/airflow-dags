#%%
# === Extraction ===
import requests
from datetime import datetime, timedelta

# Configuration
API_KEY = ""
LAT = 51.5074
LON = -0.1278

# Calculate yesterday’s window
now = datetime.utcnow()
yesterday = now - timedelta(days=1)
start_of_yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_yesterday   = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
start_ts = int(start_of_yesterday.timestamp())
end_ts   = int(end_of_yesterday.timestamp())

# Fetch
url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
params = {
    "lat": LAT,
    "lon": LON,
    "start": start_ts,
    "end": end_ts,
    "appid": API_KEY
}
print(f"📡 Fetching {start_of_yesterday} → {end_of_yesterday} UTC…")
response = requests.get(url, params=params)
response.raise_for_status()
raw_data = response.json().get("list", [])

#%%
# === Transformation ===
import pandas as pd

CITY_NAME     = "London"
LOCATION_NAME = "London Central AQ"

rows = []
for entry in raw_data:
    row = {
        "timestamp":   datetime.utcfromtimestamp(entry["dt"]),
        "city":        CITY_NAME,
        "location":    LOCATION_NAME,
        "aqi":         entry["main"]["aqi"],
        **entry["components"]
    }
    rows.append(row)

df = pd.DataFrame(rows)
df.rename(columns={"pm2_5": "pm25"}, inplace=True)
print("\n📊 Transformed DataFrame:")
print(df.head())

#%%
# === Load to raw_air_quality ===
import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    
}
TABLE_RAW = "raw_air_quality"

conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

# Create raw table
cur.execute(sql.SQL("""
CREATE TABLE IF NOT EXISTS {t} (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    city TEXT,
    location TEXT,
    aqi INTEGER,
    co FLOAT, no FLOAT, no2 FLOAT,
    o3 FLOAT, so2 FLOAT,
    pm25 FLOAT, pm10 FLOAT, nh3 FLOAT
)
""").format(t=sql.Identifier(TABLE_RAW)))
conn.commit()

# Insert into raw
insert_raw = sql.SQL("""
INSERT INTO {t}
(timestamp, city, location, aqi, co, no, no2, o3, so2, pm25, pm10, nh3)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""").format(t=sql.Identifier(TABLE_RAW))

for _, row in df.iterrows():
    cur.execute(insert_raw, (
        row.timestamp, row.city, row.location, row.aqi,
        row.get("co"), row.get("no"), row.get("no2"),
        row.get("o3"), row.get("so2"),
        row.get("pm25"), row.get("pm10"), row.get("nh3")
    ))
conn.commit()
cur.close()
conn.close()
print("✅ Loaded into raw_air_quality")

#%%
# === Load to stg_air_quality (incremental) ===
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

# Ensure staging table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS stg_air_quality (
    city            TEXT,
    location        TEXT,
    parameter       TEXT,
    clean_value     NUMERIC(10,2),
    unit            TEXT,
    measurement_date DATE
)
""")
conn.commit()

# Insert new records
cur.execute("""
INSERT INTO stg_air_quality (city, location, parameter, clean_value, unit, measurement_date)
SELECT city, location, 'pm25', ROUND(pm25::NUMERIC,2), 'µg/m³', timestamp::DATE
  FROM raw_air_quality
 WHERE pm25 IS NOT NULL
UNION ALL
SELECT city, location, 'pm10', ROUND(pm10::NUMERIC,2), 'µg/m³', timestamp::DATE
  FROM raw_air_quality
 WHERE pm10 IS NOT NULL
""")
conn.commit()
cur.close()
conn.close()
print("✅ Appended to stg_air_quality")

#%%
# === Create dimension and fact tables, deduplicate and load ===
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

# Create dim_location
print("📌 Creating dimension table 'dim_location_air_quality'...")
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_location_air_quality (
    location_id SERIAL PRIMARY KEY,
    city TEXT,
    location TEXT
)
""")

# Create fact_air_quality
print("📊 Creating fact table 'fact_air_quality'...")
cur.execute("""
CREATE TABLE IF NOT EXISTS fact_air_quality (
    location_id INT REFERENCES dim_location_air_quality(location_id),
    parameter TEXT,
    measurement_date DATE,
    avg_value DOUBLE PRECISION
)
""")
conn.commit()

# Deduplicate and load into dim_location
print("📥 Populating 'dim_location_air_quality' with unique city-location combinations...")

cur.execute("""
INSERT INTO dim_location_air_quality (city, location)
SELECT DISTINCT city, location
FROM stg_air_quality
WHERE NOT EXISTS (
    SELECT 1 FROM dim_location_air_quality d
    WHERE d.city = stg_air_quality.city
      AND d.location = stg_air_quality.location
)
""")
conn.commit()

# Load fact_air_quality from stg_air_quality
print("📈 Aggregating and loading into 'fact_air_quality'...")

cur.execute("""
INSERT INTO fact_air_quality (location_id, parameter, measurement_date, avg_value)
SELECT
    d.location_id,
    s.parameter,
    s.measurement_date,
    AVG(s.clean_value)
FROM stg_air_quality s
JOIN dim_location_air_quality d
  ON s.city = d.city AND s.location = d.location
GROUP BY d.location_id, s.parameter, s.measurement_date
""")
conn.commit()

cur.close()
conn.close()

print("✅ Dimension and Fact tables created and populated successfully.")

#%%
# === Data Quality Check + Email Alert ===
import smtplib
from email.message import EmailMessage

EXPECTED_MIN_RECORDS = 10  # Set your expected threshold here

# Email config
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_SENDER = "youremail@gmail.com"
EMAIL_PASSWORD = "your_app_password"
EMAIL_RECEIVERS = ["recipient1@example.com", "recipient2@example.com"]  # ← Multiple recipients

def send_email_alert(subject, body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_RECEIVERS)  # Join list into comma-separated string
    msg.set_content(body)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("🔍 Performing data quality checks on 'fact_air_quality'...")

    # 1. Count records
    cur.execute("SELECT COUNT(*) FROM fact_air_quality")
    row_count = cur.fetchone()[0]
    print(f"📊 Row count in 'fact_air_quality': {row_count}")

    # 2. Schema check
    cur.execute("""
    SELECT column_name, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'fact_air_quality'
    """)
    schema_info = cur.fetchall()
    print("📋 Column nullability:")
    for col, nullable in schema_info:
        print(f" - {col}: {'Nullable' if nullable == 'YES' else 'NOT NULL'}")

    # 3. NULLs in critical fields
    cur.execute("""
    SELECT COUNT(*) FROM fact_air_quality
    WHERE location_id IS NULL OR parameter IS NULL OR measurement_date IS NULL OR avg_value IS NULL
    """)
    null_issues = cur.fetchone()[0]
    if null_issues > 0:
        print(f"⚠️ Warning: Found {null_issues} records with NULLs in critical columns.")

    # 4. Volume alert
    if row_count < EXPECTED_MIN_RECORDS:
        subject = "🚨 Air Quality ETL Alert: Low Data Volume"
        body = f"""
        Alert from ETL pipeline:

        Table: fact_air_quality
        Row count: {row_count}
        Expected: {EXPECTED_MIN_RECORDS}

        Timestamp: {datetime.utcnow().isoformat()} UTC
        """
        send_email_alert(subject, body)
    else:
        print("✅ Data volume is within acceptable range.")

    cur.close()
    conn.close()

except Exception as e:
    print(f"❌ Error during data quality check: {e}")
# %%
