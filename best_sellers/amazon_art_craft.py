#%%
import requests
import time

url = "https://real-time-amazon-data.p.rapidapi.com/search"

query = "kids arts and craft"
country = "US"
sort_by = "BEST_SELLERS"
product_condition = "ALL"
is_prime = "false"
deals_and_discounts = "NONE"

headers = {
    "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

all_results = []
page = 1

while True:
    print(f"Fetching page {page}...")
    
    querystring = {
        "query": query,
        "page": str(page),
        "country": country,
        "sort_by": sort_by,
        "product_condition": product_condition,
        "is_prime": is_prime,
        "deals_and_discounts": deals_and_discounts
    }

    response = requests.get(url, headers=headers, params=querystring)
    
    if response.status_code != 200:
        print(f"Request failed with status code {response.status_code}")
        break
    
    data = response.json()
    
    products = data.get("data", {}).get("products", [])
    
    if not products:
        print("No more products found.")
        break
    
    all_results.extend(products)
    
    page += 1
    time.sleep(1)  # Respectful delay to avoid hitting rate limits

# Display or process all_results
print(f"\nTotal products fetched: {len(all_results)}")
# Example: print the first product
if all_results:
    print("\nFirst product example:")
    print(all_results[0])

# %%
import pandas as pd

# Assuming `all_results` contains the 306 products you fetched
# (from your scraping loop)

# Convert to DataFrame
df = pd.DataFrame(all_results)

# Display basic info
print("DataFrame shape:", df.shape)
print("First few rows:")
print(df.head())

# %%
# Export DataFrame to CSV
df.to_csv("kids_arts_and_crafts_products.csv", index=False)

print("Data exported to kids_arts_and_crafts_products.csv")
