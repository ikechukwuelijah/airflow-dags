#%%
import pandas as pd
import os

# Path to your Downloads folder
downloads_path = r"C:\Users\ikech\Downloads"

# Name of your CSV file (change as needed)
csv_filename = "your_file.csv"

# Full path
csv_path = os.path.join(downloads_path, csv_filename)

# Read into a DataFrame
df = pd.read_csv(csv_path)

# Inspect
print(df.head())
