import pandas as pd

FILE_PATH = "data/raw/marketing_campaign_dataset.csv"

# Load data
df = pd.read_csv(FILE_PATH)

# -------------------------
# DATA TYPE FIXES
# -------------------------

# Convert Date to datetime
df["Date"] = pd.to_datetime(df["Date"])

# Convert Acquisition_Cost to numeric
df["Acquisition_Cost"] = (
    df["Acquisition_Cost"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
)

df["Acquisition_Cost"] = pd.to_numeric(df["Acquisition_Cost"], errors="coerce")

# Convert Duration to numeric (remove text like "days" if exists)
df["Duration"] = (
    df["Duration"]
    .astype(str)
    .str.replace("days", "", regex=False)
    .str.strip()
)

df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce")

# -------------------------
# METRIC ENGINEERING
# -------------------------

# Conversions
df["Conversions"] = (df["Clicks"] * df["Conversion_Rate"]).round().astype(int)

# Spend
df["Spend"] = df["Conversions"] * df["Acquisition_Cost"]

# Revenue (ROI is multiplier)
df["Revenue"] = df["Spend"] * df["ROI"]

# CTR
df["CTR"] = df["Clicks"] / df["Impressions"]

# ROAS
df["ROAS"] = df["Revenue"] / df["Spend"]

# -------------------------
# SAVE CLEAN FILE
# -------------------------

df.to_csv("data/processed/marketing_cleaned.csv", index=False)

print("Transform complete.")
print(df.head())
