#%%
import requests
import pandas as pd

url = "https://aliexpress-business-api.p.rapidapi.com/affiliate-hot-products.php"

querystring = {"keyWord":"Human Hair","currency":"USD","lang":"en","country":"NG"}

headers = {
	"x-rapidapi-key": "",
	"x-rapidapi-host": "aliexpress-business-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())
data = response.json()

# Extract the list of items
items = data['data']['itemList']

# Convert to DataFrame
df = pd.DataFrame(items)

print(df.head())

# %%
