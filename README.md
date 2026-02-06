🛒 ELT Retail Analytics (Postgres + dbt + Power BI)

End-to-end **ELT pipeline** built around a classic analytics stack:

- **Python** for extraction + loading (raw → Postgres)
- **dbt** for transformations and a **Star Schema** (staging → marts)
- **Power BI** for the semantic model and dashboard layer

---

## 🧭 Architecture (ELT → dbt → BI)

```mermaid
flowchart TD
  A(Start) --> B(Extract and Load - Python)
  B --> C(Postgres - Docker)
  C --> D(dbt - Staging)
  D --> E(dbt - Marts Star Schema)
  E --> F(Power BI - Semantic Model)
  F --> G(Dashboard)

  subgraph dbt_models[dbt models]
    D
    E
  end
````

---

## 📌 Data scope

This dataset is filtered to:

* **France**
* **Europe (Others)**

(confirmed directly from `analytics_staging.stg_sales`).

---

## 🧱 Star Schema (Analytics Marts)

**Fact**

* `analytics_marts.fact_sales_star`
  Grain: **invoice line** (invoice_no × stock_code × date × customer)

**Dimensions**

* `analytics_marts.dim_date`
* `analytics_marts.dim_customers`
* `analytics_marts.dim_products`
* `analytics_marts.dim_invoice`

---

## 🕸️ dbt lineage / graph

The dbt graph screenshot is stored at: `assets/images/dbt_graph.png`

![dbt graph](assets/images/dbt_graph.png)

How it was produced (example):

```bash
cd dbt_retail
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir .
```

---

## 📂 Project structure

```text
ELT_retail_analytics/
├── data/
│   ├── raw/                      # source file(s) (optional in repo)
│   └── processed/                # filtered / processed exports (optional in repo)
├── dbt_retail/
│   ├── models/
│   │   ├── staging/              # stg_sales
│   │   ├── marts/core/           # dims + facts (star schema)
│   │   └── reporting/            # KPI models / views
│   ├── macros/
│   └── packages.yml
├── powerbi/
│   ├── pbix/
│   └── screenshots/
├── assets/
│   └── images/
│       └── dbt_graph.png
├── sql/
│   └── init.sql
├── docker-compose.yml
├── elt_step1_extract.py
├── .env.example
└── README.md
```

---

## ✅ Prerequisites

* Docker + Docker Compose
* Python (project uses a local virtualenv)
* `uv` installed (or adapt commands to pip)

---

## ⚙️ Setup

### 1) Create your `.env`

Copy the example and fill values:

```bash
cp .env.example .env
```

### 2) Start Postgres (Docker)

```bash
docker compose up -d
```

(Optional) verify container:

```bash
docker ps
```

### 3) Create schema / objects (if needed)

```bash
docker exec -it retail_pg psql -U retail_user -d retail -f /sql/init.sql
```

---

## 🚀 Run the pipeline

### 1) Extract + Load (Python → Postgres raw)

```bash
source .venv/Scripts/activate   # Windows Git Bash
python elt_step1_extract.py
```

### 2) Build models with dbt (staging → marts)

```bash
cd dbt_retail
set -a && source ../.env && set +a

uv run dbt run --profiles-dir . --select stg_sales dim_products dim_customers dim_date dim_invoice fact_sales fact_sales_star
```

### 3) Run tests

```bash
uv run dbt test --profiles-dir .
```

---

## 📊 Power BI (semantic model)

In Power BI Desktop:

1. **Get Data → PostgreSQL**
2. Load:

   * `analytics_marts.fact_sales_star`
   * `analytics_marts.dim_date`
   * `analytics_marts.dim_customers`
   * `analytics_marts.dim_products`
   * `analytics_marts.dim_invoice`
3. Create relationships (Many-to-one, Single direction, Active):

   * fact_sales_star[customer_id] → dim_customers[customer_id]
   * fact_sales_star[sales_date] → dim_date[date_day]
   * fact_sales_star[stock_code] → dim_products[stock_code]
   * fact_sales_star[invoice_no] → dim_invoice[invoice_no]

---

## 🔁 Refresh logic (important)

When dbt rebuilds tables/views:

* In Power BI Desktop: **Home → Refresh**
* If columns changed (new fields): **Transform data → Refresh Preview → Close & Apply**

---

## 🧪 Notes / gotchas

* For star schema relationships, dimension keys must be **unique**.
* If Power BI complains about duplicates, it can be cache/preview.
  The source-of-truth check is in Postgres (dbt tests / SQL checks).

---

## 📌 Tech stack

* Postgres (Docker)
* dbt
* Python
* Power BI
