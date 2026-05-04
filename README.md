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
enriched dataset, loads it into Amazon Redshift Serverless (SORTKEY + DISTKEY for
cost-efficient queries), transforms it with dbt into analytics-ready models, and visualizes
the results in a two-tile Metabase dashboard.

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
|   Airflow DAG      | --> |   S3 (raw zone)    | --> |  PySpark (local)   |
| (Docker Compose)   |     | 9 x CSV files      |     | join all tables    |
| 7-task pipeline    |     |                    |     | → single parquet   |
+--------------------+     +--------------------+     +--------+-----------+
                                                               |
                                                               v
                                                    +--------------------+
                                                    |  S3 (processed)    |
                                                    |  Parquet files     |
                                                    +--------+-----------+
                                                             |
                                                             v (COPY via IAM Role)
                                                    +--------------------+
                                                    | Redshift Serverless|
                                                    | olist_raw          |
                                                    | .orders_enriched   |
                                                    | (SORTKEY +         |
                                                    |  DISTKEY)          |
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
                                     dim_customer         (SORTKEY +              agg_category_performance
                                     dim_seller            DISTKEY)               agg_delivery_performance
                                     dim_date                                     agg_payment_analysis
                                                                                         |
                                                                                         v
                                                                               +--------------------+
                                                                               |  Metabase          |
                                                                               |  Dashboard (2 tiles)|
                                                                               +--------------------+
```

**Airflow DAG** (`olist_ecommerce_analytics_pipeline`):
```
download_from_kaggle → upload_raw_to_s3 → spark_transform → upload_processed_to_s3 → load_to_redshift → dbt_run → dbt_test
```

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Infrastructure as Code | Terraform | Provisions S3 bucket, Redshift Serverless namespace + workgroup, IAM role |
| Cloud | Amazon Web Services | S3, Redshift Serverless |
| Orchestration | Airflow 2.x (Docker Compose) | 7-task end-to-end DAG |
| Data Lake | Amazon S3 | Raw CSV zone + processed Parquet zone |
| Batch Processing | PySpark (local) | Joins 8 CSV tables → denormalized Parquet |
| Data Warehouse | Redshift Serverless | `SORTKEY(order_purchase_date)`, `DISTKEY(customer_state)` |
| Transformations | dbt Core (Dockerized) | staging → dimensions → facts → aggregations |
| Dashboard | Metabase (Docker) | 2 tiles: monthly trend + category bar chart |
| Containerization | Docker + Docker Compose | Airflow + dbt + Metabase |
| Testing | pytest | 4 unit tests for Spark transform |

---

## Redshift Schema

### Raw Layer — `olist_raw`

| Table | Description |
|---|---|
| `orders_enriched` | Denormalized order items from Spark. **`DISTKEY(customer_state)`** eliminates cross-node shuffles on the most common filter. **`SORTKEY(order_purchase_date)`** enables zone map pruning for all time-range queries. |

**Why DISTKEY and SORTKEY?**
- `DISTKEY(customer_state)` — distributes rows across compute nodes by state, so queries that `GROUP BY customer_state` or `JOIN` on it avoid cross-node data movement (the Redshift equivalent of BigQuery clustering).
- `SORTKEY(order_purchase_date)` — sorts data on disk by date so Redshift uses zone maps to skip irrelevant 1 MB blocks on time-range queries (the Redshift equivalent of BigQuery partitioning). Encoded as `AZ64` for ~4× compression on date values.

### Production Layer — `olist_prod` (built by dbt)

| Model | Type | Description |
|---|---|---|
| `stg_orders` | View | Cleaned, type-cast staging layer over `orders_enriched` |
| `fct_orders` | Table (SORTKEY+DISTKEY) | Fact table — one row per order item with surrogate key |
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

**Metabase Dashboard:** _(add your published link here)_

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
│   ├── Dockerfile                          # Airflow image with PySpark + dbt-redshift
│   └── dags/
│       └── olist_pipeline_dag.py           # 7-task Airflow DAG
├── dbt/
│   ├── models/
│   │   ├── staging/                        # stg_orders (view)
│   │   ├── dimensions/                     # dim_product, dim_customer, dim_seller, dim_date
│   │   ├── facts/                          # fct_orders (SORTKEY + DISTKEY table)
│   │   └── aggregations/                   # 4 aggregation tables for dashboard
│   ├── dbt_project.yml
│   ├── packages.yml                        # dbt_utils dependency
│   ├── profiles.yml
│   └── Dockerfile
├── spark/
│   └── transform_events.py                 # PySpark: join 8 CSVs → denormalized Parquet
├── scripts/
│   ├── download_data.py                    # Kaggle API download + extract
│   ├── upload_to_s3.py                     # Upload raw CSVs to S3
│   ├── upload_processed_to_s3.py           # Upload Parquet to S3
│   └── load_to_redshift.py                 # COPY Parquet from S3 into Redshift
├── terraform/
│   ├── main.tf                             # S3 bucket, Redshift Serverless, IAM role
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── tests/
│   └── test_transform.py                   # pytest unit tests for Spark transform
├── docker-compose.yml                      # Airflow + dbt + Metabase
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
- AWS account with programmatic access (Access Key ID + Secret)
- Kaggle account with API token

### 1. Clone and configure

```bash
git clone https://github.com/ParitoshDE/ecommerce-analytics.git
cd ecommerce-analytics
cp .env.example .env
```

Edit `.env` with your values:

```env
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
S3_BUCKET=olist-data-lake-yourname-2026
KAGGLE_API_TOKEN=KGAT_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 2. Provision AWS infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set s3_bucket_name and redshift_admin_password
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
terraform init
terraform plan
terraform apply
cd ..
```

This creates: S3 data lake bucket, Redshift Serverless namespace + workgroup, IAM role for S3 COPY.

After apply, copy the outputs into `.env`:
```env
REDSHIFT_HOST=<value of redshift_endpoint output>
IAM_ROLE_ARN=<value of redshift_iam_role_arn output>
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the full pipeline locally

```bash
# All steps in one command:
make all

# Or step by step:
make download          # Download Olist CSVs from Kaggle (~126 MB)
make upload            # Upload raw CSVs to S3
make spark             # PySpark transform (joins 8 tables → Parquet)
make upload-processed  # Upload Parquet to S3
make rs-load           # COPY Parquet from S3 into Redshift (SORTKEY + DISTKEY)
make dbt-run           # Run all dbt models
make dbt-test          # Run dbt data quality tests
```

### 5. Start Airflow + Metabase with Docker Compose

```bash
# Copy .env values are automatically injected
docker compose up airflow-init   # one-time DB setup + admin user creation
docker compose up -d             # start Airflow webserver, scheduler, Metabase
```

- Airflow UI: http://localhost:8080 (admin / admin)
- Metabase: http://localhost:3000

In Airflow UI: trigger the `olist_ecommerce_analytics_pipeline` DAG.

### 6. Run unit tests

```bash
make test
# or: pytest tests/ -v
```

---

## Reproducibility Checklist

- [x] All configuration via `.env` — no hardcoded credentials or project IDs
- [x] `terraform apply` provisions all AWS resources from scratch in minutes
- [x] `requirements.txt` pins all Python dependencies
- [x] `dbt/packages.yml` declares `dbt_utils` — `dbt deps` runs automatically before `dbt run`
- [x] Docker Compose for Airflow + dbt + Metabase (no local installs required beyond Docker)
- [x] `make all` runs the complete pipeline end-to-end
- [x] `pytest tests/` validates the Spark transformation
- [x] Pipeline tested on a clean clone before submission
- [x] No local paths or personal credentials hardcoded anywhere
