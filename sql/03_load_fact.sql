-- =========================
-- LOAD FACT TABLE
-- =========================

-- Make fact load idempotent for this demo:
TRUNCATE TABLE fact_campaign_daily_performance;

INSERT INTO fact_campaign_daily_performance (
  date_key, company_key, channel_key, campaign_key, location_key,
  impressions, clicks, conversions, spend, revenue, engagement_score
)
SELECT
  d.date_key,
  co.company_key,
  ch.channel_key,
  ca.campaign_key,
  lo.location_key,
  s.impressions,
  s.clicks,
  s.conversions,
  s.spend,
  s.revenue,
  s.engagement_score
FROM stg_campaign_performance s
JOIN dim_date d       ON d.date_value = s.campaign_date
JOIN dim_company co   ON co.company = s.company
JOIN dim_channel ch   ON ch.channel_used = s.channel_used
JOIN dim_campaign ca  ON ca.campaign_id = s.campaign_id
JOIN dim_location lo  ON lo.location = s.location;
