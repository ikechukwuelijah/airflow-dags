#%%
import requests
import pandas as pd

url = "https://aliexpress-business-api.p.rapidapi.com/affiliate-hot-products.php"

querystring = {"keyWord":"kids art and craft","currency":"USD","lang":"en","country":"us","filter":"best_seller"}

headers = {
	"x-rapidapi-key": "",
	"x-rapidapi-host": "aliexpress-business-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# 2. Parse JSON and normalize into flat table
data = response.json()
products = data['data']['itemList']

df = pd.json_normalize(products)

# 3. Inspect your DataFrame
print(df.shape)      # rows × columns
print(df.columns)    # all available fields
print(df.head())     # first few entries
# %%
# 4. Export to CSV
df.to_csv('aliexpresskids_arts_and_crafts_products.csv', index=False)
# %%
