-- =========================
-- LOAD DIMENSIONS FROM STAGING
-- =========================

-- dim_date
INSERT INTO dim_date (date_key, date_value, year, month, day, week)
SELECT DISTINCT
  (EXTRACT(YEAR FROM campaign_date)::int * 10000
   + EXTRACT(MONTH FROM campaign_date)::int * 100
   + EXTRACT(DAY FROM campaign_date)::int) AS date_key,
  campaign_date AS date_value,
  EXTRACT(YEAR FROM campaign_date)::int AS year,
  EXTRACT(MONTH FROM campaign_date)::int AS month,
  EXTRACT(DAY FROM campaign_date)::int AS day,
  EXTRACT(WEEK FROM campaign_date)::int AS week
FROM stg_campaign_performance
ON CONFLICT (date_value) DO NOTHING;

-- dim_company
INSERT INTO dim_company (company)
SELECT DISTINCT company
FROM stg_campaign_performance
ON CONFLICT (company) DO NOTHING;

-- dim_channel
INSERT INTO dim_channel (channel_used)
SELECT DISTINCT channel_used
FROM stg_campaign_performance
ON CONFLICT (channel_used) DO NOTHING;

-- dim_location
INSERT INTO dim_location (location)
SELECT DISTINCT location
FROM stg_campaign_performance
ON CONFLICT (location) DO NOTHING;

-- dim_campaign
INSERT INTO dim_campaign (campaign_id, campaign_type, target_audience, duration_days, language, customer_segment)
SELECT DISTINCT
  campaign_id,
  campaign_type,
  target_audience,
  duration_days,
  language,
  customer_segment
FROM stg_campaign_performance
ON CONFLICT (campaign_id) DO NOTHING;
