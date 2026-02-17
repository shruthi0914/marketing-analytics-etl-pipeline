import os
import pandas as pd
from sqlalchemy import create_engine, text

# Path is relative to project root (/opt/airflow/project inside Docker)
CSV_PATH = os.getenv("CSV_PATH", "data/processed/marketing_cleaned.csv")

# Database connection (safe defaults for Airflow-in-Docker)
DB_NAME = os.getenv("PGDATABASE", "marketing_analytics")
DB_USER = os.getenv("PGUSER", "etl_user")
DB_PASSWORD = os.getenv("PGPASSWORD", "etl_pass")
DB_HOST = os.getenv("PGHOST", "host.docker.internal")
DB_PORT = os.getenv("PGPORT", "5432")


def make_engine():
    # Always use password-based auth for Docker/remote connections
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def main():
    print(f"[load_to_staging] Using CSV_PATH={CSV_PATH}")
    print(f"[load_to_staging] Connecting to DB host={DB_HOST} port={DB_PORT} db={DB_NAME} user={DB_USER}")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Cleaned CSV not found at: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    # Align columns to staging table names (must match your CREATE TABLE script)
    df = df.rename(
        columns={
            "Campaign_ID": "campaign_id",
            "Company": "company",
            "Campaign_Type": "campaign_type",
            "Target_Audience": "target_audience",
            "Duration": "duration_days",
            "Channel_Used": "channel_used",
            "Conversion_Rate": "conversion_rate",
            "Acquisition_Cost": "acquisition_cost",
            "ROI": "roi_multiplier",
            "Location": "location",
            "Language": "language",
            "Clicks": "clicks",
            "Impressions": "impressions",
            "Engagement_Score": "engagement_score",
            "Customer_Segment": "customer_segment",
            "Date": "campaign_date",
            "Conversions": "conversions",
            "Spend": "spend",
            "Revenue": "revenue",
            "CTR": "ctr",
            "ROAS": "roas",
        }
    )

    # Ensure campaign_date is a proper date
    df["campaign_date"] = pd.to_datetime(df["campaign_date"]).dt.date

    engine = make_engine()

    # Idempotent reload of staging
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_campaign_performance;"))

    # Bulk insert via pandas
    df.to_sql(
        "stg_campaign_performance",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print(f"[load_to_staging] Loaded {len(df):,} rows into stg_campaign_performance")


if __name__ == "__main__":
    main()
