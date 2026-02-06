# 🛍️ ELT Retail Analytics — dbt + Postgres + Power BI (Star Schema)

End-to-end ELT project: ingestion → staging → marts → reporting, with a **Star Schema (Gold layer)** designed for BI consumption.

## 🧱 Architecture

**Raw (Postgres)**

- `raw.staging_sales` : ingestion table (Online Retail)

**Staging (dbt)**

- `analytics_staging.stg_sales` : cleaned/enriched view (flags returns/cancellations, standardized fields)

**Marts (dbt)**

- `analytics_marts.fact_sales` : fact table (invoice line level)
- `analytics_marts.dim_customers`, `analytics_marts.dim_products`
- `analytics_marts.dim_date` : date dimension generated from min/max invoice_date
- `analytics_marts.fact_sales_star` : fact with `sales_date`

**Gold / BI-ready (dbt)**

- `analytics_reporting.sales_star` : denormalized star view for Power BI
- KPI tables:
  - `analytics_reporting.kpi_daily`
  - `analytics_reporting.kpi_product`
  - `analytics_reporting.kpi_customer`

## ✅ Data Quality

dbt tests:

- not_null / unique on key columns
- relationships tests (fact → dimensions)
- dbt_utils unique combination on product keys

## 🚀 How to run (local)

### 1) Start Postgres (Docker)

```bash
docker compose up -d
```
