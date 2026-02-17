-- =========
-- STAGING
-- =========
DROP TABLE IF EXISTS stg_campaign_performance;

CREATE TABLE stg_campaign_performance (
  campaign_id        INTEGER,
  company            TEXT,
  campaign_type      TEXT,
  target_audience    TEXT,
  duration_days      INTEGER,
  channel_used       TEXT,
  conversion_rate    DOUBLE PRECISION,
  acquisition_cost   DOUBLE PRECISION,
  roi_multiplier     DOUBLE PRECISION,
  location           TEXT,
  language           TEXT,
  clicks             INTEGER,
  impressions        INTEGER,
  engagement_score   INTEGER,
  customer_segment   TEXT,
  campaign_date      DATE,

  -- engineered fields
  conversions        INTEGER,
  spend              DOUBLE PRECISION,
  revenue            DOUBLE PRECISION,
  ctr                DOUBLE PRECISION,
  roas               DOUBLE PRECISION
);

-- =========
-- DIMENSIONS
-- =========
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
  date_key     INTEGER PRIMARY KEY,
  date_value   DATE UNIQUE,
  year         INTEGER,
  month        INTEGER,
  day          INTEGER,
  week         INTEGER
);

DROP TABLE IF EXISTS dim_company CASCADE;
CREATE TABLE dim_company (
  company_key  SERIAL PRIMARY KEY,
  company      TEXT UNIQUE
);

DROP TABLE IF EXISTS dim_channel CASCADE;
CREATE TABLE dim_channel (
  channel_key  SERIAL PRIMARY KEY,
  channel_used TEXT UNIQUE
);

DROP TABLE IF EXISTS dim_campaign CASCADE;
CREATE TABLE dim_campaign (
  campaign_key   SERIAL PRIMARY KEY,
  campaign_id    INTEGER UNIQUE,
  campaign_type  TEXT,
  target_audience TEXT,
  duration_days  INTEGER,
  language       TEXT,
  customer_segment TEXT
);

DROP TABLE IF EXISTS dim_location CASCADE;
CREATE TABLE dim_location (
  location_key SERIAL PRIMARY KEY,
  location     TEXT UNIQUE
);

-- =========
-- FACT TABLE
-- =========
DROP TABLE IF EXISTS fact_campaign_daily_performance;

CREATE TABLE fact_campaign_daily_performance (
  fact_key      SERIAL PRIMARY KEY,
  date_key      INTEGER REFERENCES dim_date(date_key),
  company_key   INTEGER REFERENCES dim_company(company_key),
  channel_key   INTEGER REFERENCES dim_channel(channel_key),
  campaign_key  INTEGER REFERENCES dim_campaign(campaign_key),
  location_key  INTEGER REFERENCES dim_location(location_key),

  impressions   INTEGER,
  clicks        INTEGER,
  conversions   INTEGER,
  spend         DOUBLE PRECISION,
  revenue       DOUBLE PRECISION,
  engagement_score INTEGER
);
