-- =========================================================
-- KPI VIEWS (Dashboard-ready)
-- Use CREATE OR REPLACE so Airflow can rerun safely
-- =========================================================

-- 1) Daily KPIs by Channel
CREATE OR REPLACE VIEW vw_daily_channel_kpis AS
SELECT
  d.date_value AS date,
  ch.channel_used AS channel,
  SUM(f.impressions) AS impressions,
  SUM(f.clicks) AS clicks,
  SUM(f.conversions) AS conversions,
  SUM(f.spend) AS spend,
  SUM(f.revenue) AS revenue,

  CASE WHEN SUM(f.impressions) = 0 THEN 0
       ELSE SUM(f.clicks)::double precision / SUM(f.impressions) END AS ctr,

  CASE WHEN SUM(f.clicks) = 0 THEN 0
       ELSE SUM(f.conversions)::double precision / SUM(f.clicks) END AS cvr,

  CASE WHEN SUM(f.conversions) = 0 THEN NULL
       ELSE SUM(f.spend)::double precision / SUM(f.conversions) END AS cpa,

  CASE WHEN SUM(f.clicks) = 0 THEN NULL
       ELSE SUM(f.spend)::double precision / SUM(f.clicks) END AS cpc,

  CASE WHEN SUM(f.spend) = 0 THEN NULL
       ELSE SUM(f.revenue)::double precision / SUM(f.spend) END AS roas
FROM fact_campaign_daily_performance f
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_channel ch  ON ch.channel_key = f.channel_key
GROUP BY d.date_value, ch.channel_used;


-- 2) Monthly KPIs by Channel (nice for exec dashboards)
CREATE OR REPLACE VIEW vw_monthly_channel_kpis AS
SELECT
  (d.year::text || '-' || LPAD(d.month::text, 2, '0')) AS year_month,
  ch.channel_used AS channel,
  SUM(f.impressions) AS impressions,
  SUM(f.clicks) AS clicks,
  SUM(f.conversions) AS conversions,
  SUM(f.spend) AS spend,
  SUM(f.revenue) AS revenue,

  CASE WHEN SUM(f.impressions) = 0 THEN 0
       ELSE SUM(f.clicks)::double precision / SUM(f.impressions) END AS ctr,

  CASE WHEN SUM(f.clicks) = 0 THEN 0
       ELSE SUM(f.conversions)::double precision / SUM(f.clicks) END AS cvr,

  CASE WHEN SUM(f.spend) = 0 THEN NULL
       ELSE SUM(f.revenue)::double precision / SUM(f.spend) END AS roas
FROM fact_campaign_daily_performance f
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_channel ch  ON ch.channel_key = f.channel_key
GROUP BY d.year, d.month, ch.channel_used;


-- 3) Company-level KPIs (who drives revenue/ROAS)
CREATE OR REPLACE VIEW vw_company_kpis AS
SELECT
  co.company,
  SUM(f.impressions) AS impressions,
  SUM(f.clicks) AS clicks,
  SUM(f.conversions) AS conversions,
  SUM(f.spend) AS spend,
  SUM(f.revenue) AS revenue,

  CASE WHEN SUM(f.impressions) = 0 THEN 0
       ELSE SUM(f.clicks)::double precision / SUM(f.impressions) END AS ctr,

  CASE WHEN SUM(f.clicks) = 0 THEN 0
       ELSE SUM(f.conversions)::double precision / SUM(f.clicks) END AS cvr,

  CASE WHEN SUM(f.spend) = 0 THEN NULL
       ELSE SUM(f.revenue)::double precision / SUM(f.spend) END AS roas
FROM fact_campaign_daily_performance f
JOIN dim_company co ON co.company_key = f.company_key
GROUP BY co.company;


-- 4) Campaign performance summary (Top/Bottom campaigns)
CREATE OR REPLACE VIEW vw_campaign_kpis AS
SELECT
  ca.campaign_id,
  ca.campaign_type,
  ca.target_audience,
  ca.duration_days,
  ca.language,
  ca.customer_segment,
  SUM(f.impressions) AS impressions,
  SUM(f.clicks) AS clicks,
  SUM(f.conversions) AS conversions,
  SUM(f.spend) AS spend,
  SUM(f.revenue) AS revenue,

  CASE WHEN SUM(f.impressions) = 0 THEN 0
       ELSE SUM(f.clicks)::double precision / SUM(f.impressions) END AS ctr,

  CASE WHEN SUM(f.clicks) = 0 THEN 0
       ELSE SUM(f.conversions)::double precision / SUM(f.clicks) END AS cvr,

  CASE WHEN SUM(f.spend) = 0 THEN NULL
       ELSE SUM(f.revenue)::double precision / SUM(f.spend) END AS roas
FROM fact_campaign_daily_performance f
JOIN dim_campaign ca ON ca.campaign_key = f.campaign_key
GROUP BY
  ca.campaign_id, ca.campaign_type, ca.target_audience,
  ca.duration_days, ca.language, ca.customer_segment;


-- 5) Location KPIs (geo performance)
CREATE OR REPLACE VIEW vw_location_kpis AS
SELECT
  lo.location,
  SUM(f.spend) AS spend,
  SUM(f.revenue) AS revenue,
  SUM(f.conversions) AS conversions,
  CASE WHEN SUM(f.spend) = 0 THEN NULL
       ELSE SUM(f.revenue)::double precision / SUM(f.spend) END AS roas
FROM fact_campaign_daily_performance f
JOIN dim_location lo ON lo.location_key = f.location_key
GROUP BY lo.location;


-- 6) Underperforming campaigns (rule-based flags)
CREATE OR REPLACE VIEW vw_underperforming_campaigns AS
SELECT *
FROM vw_campaign_kpis
WHERE
  roas IS NOT NULL
  AND roas < 3.5
  AND spend > 0
ORDER BY roas ASC, spend DESC;


-- 7) Executive summary (single-row totals)
CREATE OR REPLACE VIEW vw_exec_summary AS
SELECT
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(conversions) AS total_conversions,
  SUM(spend) AS total_spend,
  SUM(revenue) AS total_revenue,
  CASE WHEN SUM(impressions) = 0 THEN 0
       ELSE SUM(clicks)::double precision / SUM(impressions) END AS ctr,
  CASE WHEN SUM(clicks) = 0 THEN 0
       ELSE SUM(conversions)::double precision / SUM(clicks) END AS cvr,
  CASE WHEN SUM(spend) = 0 THEN NULL
       ELSE SUM(revenue)::double precision / SUM(spend) END AS roas
FROM fact_campaign_daily_performance;
