# Olist E-Commerce Analytics

End-to-end batch data pipeline for the **Olist Brazilian E-Commerce** public dataset,
built as the capstone project for the **Data Engineering Zoomcamp 2026**.

---

## Problem Statement

Brazilian e-commerce grew explosively between 2016 and 2018, but businesses lacked
visibility into key operational metrics. This project answers four concrete business questions:

1. **Growth trend** — How does monthly order volume and revenue change over time?
   Are there seasonal peaks (e.g. Black Friday)?
2. **Category performance** — Which product categories generate the most revenue and
   highest customer satisfaction scores?
3. **Delivery reliability** — Which Brazilian states have the worst delivery performance
   and highest average delay?
4. **Payment preferences** — How do payment methods (credit card, boleto, voucher,
   debit card) compare in average order value?

The pipeline ingests 9 raw CSV files from Kaggle, joins them with PySpark into a single
enriched dataset, loads it into BigQuery (partitioned + clustered for cost-efficient queries),
transforms it with dbt into analytics-ready models, and visualizes the results in a
two-tile Looker Studio dashboard.

---

## Dataset

| Property | Value |
|---|---|
| Source | Olist Brazilian E-Commerce Public Dataset |
| Provider | Kaggle — `olistbr/brazilian-ecommerce` |
| URL | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Period | September 2016 — October 2018 |
| Size | ~126 MB, 9 CSV files, ~100,000 orders |
| License | CC BY-NC-SA 4.0 |

The dataset contains real anonymized commercial data from Olist, Brazil's largest
department store marketplace, connecting small businesses to customers across the country.

---

## Architecture

```
Kaggle API (olistbr/brazilian-ecommerce)
         |
         v
+--------------------+     +--------------------+     +--------------------+
|   Airflow DAG      | --> |   GCS (raw zone)   | --> |  PySpark           |
| (Cloud Composer)   |     | 9 x CSV files      |     | join all tables    |
| 6-task pipeline    |     |                    |     | → single parquet   |
+--------------------+     +--------------------+     +--------+-----------+
                                                               |
                                                               v
                                                    +--------------------+
                                                    |  GCS (processed)   |
                                                    |  Parquet files     |
                                                    +--------+-----------+
                                                             |
                                                             v
                                                    +--------------------+
                                                    |    BigQuery        |
                                                    | olist_raw          |
                                                    | .orders_enriched   |
                                                    | (partitioned +     |
                                                    |  clustered)        |
                                                    +--------+-----------+
                                                             |
                                                             v
                                                          +-----+
                                                          | dbt |
                                                          +--+--+
                                                             |
                          +----------------------------------+----------------------------------+
                          |                  |               |                                 |
                    staging/          dimensions/          facts/                   aggregations/
                   stg_orders        dim_product          fct_orders              agg_monthly_orders
                                     dim_customer         (partitioned            agg_category_performance
                                     dim_seller            + clustered)           agg_delivery_performance
                                     dim_date                                     agg_payment_analysis
                                                                                         |
                                                                                         v
                                                                               +--------------------+
                                                                               |  Looker Studio     |
                                                                               |  Dashboard (2 tiles)|
                                                                               +--------------------+
```

**Airflow DAG** (`olist_ecommerce_analytics_pipeline`):
```
download_from_kaggle → upload_raw_to_gcs → spark_transform → load_to_bigquery → dbt_run → dbt_test
```

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Infrastructure as Code | Terraform | Provisions GCS bucket, BigQuery datasets, service account + IAM |
| Cloud | Google Cloud Platform | GCS, BigQuery, Cloud Composer, Dataproc Serverless |
| Orchestration | Airflow 2.x (Cloud Composer) | 6-task end-to-end DAG |
| Data Lake | Google Cloud Storage | Raw CSV zone + processed Parquet zone |
| Batch Processing | PySpark (Dataproc Serverless) | Joins 8 CSV tables → denormalized Parquet |
| Data Warehouse | BigQuery | Partitioned by `order_purchase_date`, clustered by `customer_state` + `product_category_name_english` |
| Transformations | dbt Core (Dockerized) | staging → dimensions → facts → aggregations |
| Dashboard | Looker Studio | 2 tiles: monthly trend + category bar chart |
| Containerization | Docker + Docker Compose | Local dbt execution |
| Testing | pytest | 4 unit tests for Spark transform |

---

## BigQuery Schema

### Raw Layer — `olist_raw`

| Table | Description |
|---|---|
| `orders_enriched` | Denormalized order items from Spark. **Partitioned** by `order_purchase_date` (day), **clustered** by `customer_state`, `product_category_name_english`. |

**Why this partitioning and clustering?**
- Partitioning by `order_purchase_date` optimizes all time-range queries (e.g. "revenue this month") — BigQuery only scans the relevant partitions instead of the full table.
- Clustering by `customer_state` + `product_category_name_english` optimizes the two most common GROUP BY patterns in analytical queries, reducing bytes scanned and query cost significantly.

### Production Layer — `olist_prod` (built by dbt)

| Model | Type | Description |
|---|---|---|
| `stg_orders` | View | Cleaned, type-cast staging layer over `orders_enriched` |
| `fct_orders` | Partitioned Table | Fact table — one row per order item with surrogate key |
| `dim_product` | Table | Product category dimension |
| `dim_customer` | Table | Customer dimension by state/city |
| `dim_seller` | Table | Seller dimension |
| `dim_date` | Table | Calendar dimension |
| `agg_monthly_orders` | Table | Monthly order volume, revenue, delivery performance |
| `agg_category_performance` | Table | Revenue and review scores by product category |
| `agg_delivery_performance` | Table | On-time delivery rates by customer state |
| `agg_payment_analysis` | Table | Payment method breakdown |

---

## Dashboard

**Looker Studio Dashboard:** _(add your published link here)_

**Tile 1 — Monthly Order Volume and Revenue (2016–2018)**
- Source: `olist_prod.agg_monthly_orders`
- Chart type: Combo chart (bars = total_orders, line = total_revenue)
- Shows month-over-month growth and seasonal peaks (Black Friday Nov 2017 spike)

**Tile 2 — Top Product Categories by Revenue**
- Source: `olist_prod.agg_category_performance`
- Chart type: Horizontal bar chart, top 20 categories
- Color-coded by `avg_review_score` to show which high-revenue categories have satisfaction issues

---

## Project Structure

```
ecommerce-analytics/
├── airflow/
│   └── dags/
│       └── olist_pipeline_dag.py       # 6-task Airflow DAG
├── dbt/
│   ├── models/
│   │   ├── staging/                    # stg_orders (view)
│   │   ├── dimensions/                 # dim_product, dim_customer, dim_seller, dim_date
│   │   ├── facts/                      # fct_orders (partitioned + clustered table)
│   │   └── aggregations/               # 4 aggregation tables for dashboard
│   ├── dbt_project.yml
│   ├── packages.yml                    # dbt_utils dependency
│   ├── profiles.yml
│   └── Dockerfile
├── spark/
│   └── transform_events.py             # PySpark: join 8 CSVs → denormalized Parquet
├── scripts/
│   ├── download_data.py                # Kaggle API download + extract
│   ├── upload_to_gcs.py                # Upload raw CSVs to GCS
│   └── load_to_bigquery.py             # Load processed Parquet to BigQuery
├── terraform/
│   ├── main.tf                         # GCS bucket, BigQuery datasets, SA, IAM
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── tests/
│   └── test_transform.py               # pytest unit tests for Spark transform
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── .env.example
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- Docker and Docker Compose
- Terraform >= 1.5
- GCP project with billing enabled
- GCP service account key JSON (BigQuery Admin + Storage Admin + Dataproc Editor roles)
- Kaggle account with API token

### 1. Clone and configure

```bash
git clone https://github.com/ParitoshDE/ecommerce-analytics.git
cd ecommerce-analytics
cp .env.example .env
```

Edit `.env` with your values:

```env
GCP_PROJECT_ID=my-gcp-project
GCS_BUCKET=my-gcp-project-olist-data-lake
KAGGLE_API_TOKEN=KGAT_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
GOOGLE_APPLICATION_CREDENTIALS=./keys/gcp-service-account.json
```

Place your GCP service account key at `keys/gcp-service-account.json`.

### 2. Provision GCP infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set project_id and data_lake_bucket_name
terraform init
terraform plan
terraform apply
cd ..
```

This creates: GCS bucket, BigQuery datasets (`olist_raw`, `olist_prod`), service account with required IAM roles.

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the full pipeline locally

```bash
# All steps in one command:
make all

# Or step by step:
make download    # Download Olist CSVs from Kaggle (~126 MB)
make upload      # Upload raw CSVs to GCS
make spark       # PySpark transform (joins 8 tables → Parquet)
make bq-load     # Load Parquet to BigQuery (partitioned + clustered)
make dbt-run     # Run all dbt models
make dbt-test    # Run dbt data quality tests
```

### 5. Run dbt with Docker Compose

```bash
docker-compose up
```

### 6. Run unit tests

```bash
make test
# or: pytest tests/ -v
```

### 7. Deploy to Cloud Composer (Airflow)

```bash
# Copy DAG to Composer GCS bucket
gsutil cp airflow/dags/olist_pipeline_dag.py gs://<your-composer-bucket>/dags/

# Set Airflow Variables in the Composer UI (Admin → Variables):
# GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET
# BQ_RAW_DATASET=olist_raw, BQ_PROD_DATASET=olist_prod
# KAGGLE_API_TOKEN, GOOGLE_APPLICATION_CREDENTIALS
# PIPELINE_SERVICE_ACCOUNT=olist-pipeline-sa@<project>.iam.gserviceaccount.com
# COMPOSER_REPO_ROOT_GCS=gs://<composer-bucket>/dags/ecommerce-analytics

# Trigger the DAG:
# Airflow UI → DAGs → olist_ecommerce_analytics_pipeline → Trigger
```

---

## Reproducibility Checklist

- [x] All configuration via `.env` — no hardcoded credentials or project IDs
- [x] `terraform apply` provisions all GCP resources from scratch in minutes
- [x] `requirements.txt` pins all Python dependencies
- [x] `dbt/packages.yml` declares `dbt_utils` — `dbt deps` runs automatically before `dbt run`
- [x] Docker Compose for local dbt execution (no local dbt install required)
- [x] `make all` runs the complete pipeline end-to-end
- [x] `pytest tests/` validates the Spark transformation
- [x] Pipeline tested on a clean clone before submission
- [x] No local paths or personal project IDs hardcoded anywhere
