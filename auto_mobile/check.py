#%%
import requests

url = "https://car-api2.p.rapidapi.com/api/models"

querystring = {"sort":"id","direction":"asc","year":"2025"}

headers = {
	"x-rapidapi-key": "",
	"x-rapidapi-host": "car-api2.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

#%% convert to pandas dataframe
import pandas as pd
data = response.json()
# 1) Inspect what top-level keys you actually have:
print("Top-level keys:", data.keys())

# 2) Try to find the list automatically (first value that is a list):
models_list = next(
    (value for value in data.values() if isinstance(value, list)),
    None
)

if models_list is None:
    raise ValueError("Couldn't find any list in the JSON response—check the structure manually!")

# 3) Convert that list of dicts into a DataFrame:
df = pd.DataFrame(models_list)
print(df.head())

# %%
