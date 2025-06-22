#%%
import requests
import pandas as pd

# 1. Your existing request setup
url = "https://aliexpress-business-api.p.rapidapi.com/textsearch.php"
querystring = {
    "keyWord":"kids art and craft",
    "pageSize":"20",
    "country":"US",
    "currency":"USD",
    "lang":"en",
    "filter":"orders",
    "sortBy":"asc",
    "categoryId":"best seller"
}
headers = {
    "x-rapidapi-key": "", 
    "x-rapidapi-host": "aliexpress-business-api.p.rapidapi.com"
}

resp = requests.get(url, headers=headers, params=querystring)
resp_json = resp.json()

# 2. Find the first list-of-dicts inside the JSON (usually products)
data_section = resp_json.get("data", resp_json)
product_list = None
for key, val in data_section.items():
    if isinstance(val, list) and val and isinstance(val[0], dict):
        product_list = val
        print(f"▶️ Using list at data['{key}']")
        break

if product_list is None:
    raise RuntimeError("Could not find a list of product entries in the response")

# 3. Normalize into a flat table
df = pd.json_normalize(product_list)

# 4. Inspect / save
print(f"Loaded {len(df)} products with {len(df.columns)} fields")
print(df.columns.tolist())   # all available columns
print(df.head())             # preview

# 5. (Optional) export
df.to_csv("aliexpress2_kids_art_craft.csv", index=False)

# %%
