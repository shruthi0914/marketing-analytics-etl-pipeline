# Marketing Analytics ETL Pipeline
**End-to-End Data Engineering Project using Python, PostgreSQL, Airflow & Docker**

---

## Project Overview

This project builds a production-style Marketing Analytics data pipeline that:

- Cleans and transforms raw campaign data using Python
- Loads data into a PostgreSQL data warehouse
- Implements a Star Schema (Fact + Dimensions)
- Creates business-ready KPI views (semantic layer)
- Automates the full workflow using Apache Airflow
- Runs inside Docker for reproducibility and portability

This project demonstrates real-world data engineering practices including idempotent ETL design, dimensional modeling, orchestration, database permissions handling, and infrastructure debugging.

---

##  Architecture Overview
Raw CSV (Marketing Dataset)
↓
Python Transformation (Pandas)
↓
Processed Cleaned Data
↓
Postgres Staging Table
↓
Star Schema (Dimensions + Fact Table)
↓
KPI Views (Semantic Layer)
↓
Airflow DAG Automation

---

## 🛠️ Tech Stack

- **Python** (Pandas, SQLAlchemy)
- **PostgreSQL**
- **Apache Airflow (Dockerized)**
- **Docker & Docker Compose**
- **SQL (Dimensional Modeling)**
- **Git & GitHub**

---

## 📁 Project Structure

marketing-analytics-etl-pipeline/
│
├── dags/
│ └── marketing_pipeline.py
│
├── src/
│ ├── transform_data.py
│ └── load_to_staging.py
│
├── sql/
│ ├── 01_create_tables.sql
│ ├── 02_load_dimensions.sql
│ ├── 03_load_fact.sql
│ └── 04_kpi_views.sql
│
├── data/
│ ├── raw/ # Ignored in Git
│ └── processed/ # Ignored in Git
│
├── Dockerfile.airflow
├── docker-compose.yaml
├── requirements.airflow.txt
├── .gitignore
└── README.md

---

## Airflow DAG Workflow

DAG: `marketing_analytics_pipeline`

### Task Order:

1. `transform_csv`
2. `load_to_staging`
3. `load_dimensions`
4. `load_fact`
5. `refresh_kpi_views`

Each task is modular, rerunnable, and designed to support safe retries.

---

##  Data Modeling Strategy

### Staging Layer

**Table:** `stg_campaign_performance`

- Stores cleaned and standardized data
- Reloaded idempotently using `TRUNCATE + INSERT`
- Acts as a replayable landing zone

---

### Star Schema (Warehouse Layer)

#### Dimension Tables

- `dim_date`
- `dim_company`
- `dim_channel`
- `dim_campaign`
- `dim_location`

Each dimension stores descriptive attributes once, reducing redundancy.

---

#### Fact Table

`fact_campaign_daily_performance`

Stores measurable metrics:

- impressions
- clicks
- conversions
- spend
- revenue
- foreign keys to dimensions

This structure optimizes analytical queries and dashboard performance.

---

## KPI Semantic Layer

Business-ready views built on top of the warehouse:

- `vw_exec_summary`
- `vw_daily_channel_kpis`
- `vw_monthly_channel_kpis`
- `vw_company_kpis`
- `vw_campaign_kpis`
- `vw_location_kpis`
- `vw_underperforming_campaigns`

Standardized KPIs include:

- CTR (Click-Through Rate)
- CVR (Conversion Rate)
- ROAS (Return on Ad Spend)
- CPA (Cost per Acquisition)
- CPC (Cost per Click)

These views ensure consistent KPI definitions across reporting tools.

---

## Production Engineering Practices Implemented

- Idempotent staging loads
- Conflict-safe dimension inserts (`ON CONFLICT DO NOTHING`)
- Controlled fact loading
- `CREATE OR REPLACE VIEW` for semantic layer refresh
- Airflow retry configuration
- Dedicated `etl_user` for warehouse operations
- Proper database ownership management
- Dockerized reproducible environment
- Separation of transformation, modeling, and orchestration logic

---

## Running the Project

### Start Airflow Services

```bash
docker compose up airflow-init
docker compose up
```
### Access Airflow UI
http://localhost:8080

### Login credentials:
Username: admin
Password: admin

### Trigger the DAG
Run: marketing_analytics_pipeline

### Real-World Issues Solved During Development
This project involved solving real production-style issues:
Docker networking differences (localhost vs host.docker.internal)
Airflow secret key configuration for log access
SQLAlchemy compatibility with Airflow
Airflow connection hook type mismatch
Postgres ownership & permission conflicts
Idempotent pipeline design
DAG file path & container volume mounting issues
View ownership conflicts when refreshing semantic layer
These reflect real enterprise data engineering environments.

### Example Business Insights Enabled
-Using KPI views, stakeholders can:
-Identify underperforming campaigns (ROAS threshold logic)
-Compare channel-level efficiency
-Track monthly performance trends
-Evaluate company-level revenue contribution
-Generate executive summary metrics

### Example Business Insights Enabled
-Using KPI views, stakeholders can:
-Identify underperforming campaigns (ROAS threshold logic)
-Compare channel-level efficiency
-Track monthly performance trends
-Evaluate company-level revenue contribution
-Generate executive summary metrics


