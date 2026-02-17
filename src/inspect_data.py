import pandas as pd

FILE_PATH = "/Users/shruthirajgangapuri/myproject/Marketing_Analytics/data/raw/marketing_campaign_dataset.csv"

# Load data
df = pd.read_csv(FILE_PATH)

print("\n===== COLUMN NAMES =====")
print(df.columns)

print("\n===== FIRST 5 ROWS =====")
print(df.head())

print("\n===== DATA INFO =====")
print(df.info())

print("\n===== NULL VALUES =====")
print(df.isnull().sum().sort_values(ascending=False))
