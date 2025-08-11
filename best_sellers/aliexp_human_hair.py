#%%
import requests
import pandas as pd

url = "https://aliexpress-business-api.p.rapidapi.com/affiliate-hot-products.php"

querystring = {"keyWord":"Human Hair","currency":"USD","lang":"en","country":"NG"}

headers = {
	"x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
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
