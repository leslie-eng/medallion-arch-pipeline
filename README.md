# Retail Store Data Warehouse — Medallion Architecture

An end-to-end batch data pipeline that ingests the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from an S3-compatible object store (MinIO) into a PostgreSQL data warehouse, orchestrated with Apache Airflow and modelled using the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture

```
┌──────────────────┐      ┌────────────────────────────────────────────────────────────┐
│  MinIO (S3)      │      │  PostgreSQL — Retail_Store_DataWarehouse                   │
│                  │      │                                                            │
│  ecommercedataset│      │  ┌──────────┐    ┌──────────┐    ┌──────────────────────┐  │
│   ├── data/      │─────▶│  │  BRONZE  │───▶│  SILVER  │───▶│        GOLD          │  │
│   │   *.csv      │      │  │ raw load │    │ cleaned, │    │ snowflake (views)    │  │
│   └── archive/   │◀─┐   │  │ as-is    │    │ typed    │    │ dims + fact          │  │
└──────────────────┘  │   │  └──────────┘    └──────────┘    └──────────────────────┘  │
                      │   └────────────────────────────────────────────────────────────┘
                      │                          ▲
                      │   ┌──────────────────────┴────────┐
                      └───│  Apache Airflow 3 (LocalExec) │
            archive files │  DAG: s3_to_postgres_v010     │
                          └───────────────────────────────┘
```

Detailed diagrams (draw.io) are in the parent folder:

- [`medallion architecture.drawio`](../medallion%20architecture.drawio): source files → Bronze layer
- [`airflow to sql server.drawio`](../airflow%20to%20sql%20server.drawio): orchestration flow

## Tech Stack

| Component        | Technology                          | Purpose                                   |
|------------------|-------------------------------------|-------------------------------------------|
| Orchestration    | Apache Airflow 3.3 (LocalExecutor)  | Scheduling and running the pipeline       |
| Object storage   | MinIO (S3-compatible)               | Landing zone for raw CSV files            |
| Data warehouse   | PostgreSQL 16                       | Bronze / Silver / Gold schemas            |
| Processing       | Python, pandas, PL/pgSQL            | Chunked ingestion and SQL transformations |
| Containerisation | Docker Compose                      | Local runtime for all services            |
| Package manager  | uv                                  | Python dependency management              |

## Project Structure

```
medallion/
├── dags/
│   └── dag_retail_store.py      # Airflow DAG: bronze → silver → gold → archive
├── sql/                         # Mounted into Airflow at /opt/airflow/sql
│   ├── Bronze_layer.sql         # Procedure bronze_layer_creation(): (re)creates the bronze tables
│   ├── silver layer.sql         # Procedure silver.load_silver(): clean + load silver
│   └── gold_layer.sql           # Snowflake-schema views (fact + dimensions)
├── config/airflow.cfg           # Airflow configuration
├── docker-compose.yaml          # Airflow + Postgres + MinIO services
├── main.py                      # Stand-alone script to test MinIO connectivity
├── pyproject.toml / uv.lock     # Python dependencies
└── .env                         # Environment variables (not committed)
```

## Source Data

Nine CSV files from the Olist dataset are uploaded to the `ecommercedataset` bucket under the `data/` prefix:

| File                                     | Description                              |
|------------------------------------------|------------------------------------------|
| `olist_customers_dataset.csv`            | Customers and their location             |
| `olist_geolocation_dataset.csv`          | Zip-code prefix → lat/lng, city, state   |
| `olist_orders_dataset.csv`               | Orders and their lifecycle timestamps    |
| `olist_order_items_dataset.csv`          | Items within each order                  |
| `olist_order_payments_dataset.csv`       | Payment method, installments and value   |
| `olist_order_reviews_dataset.csv`        | Customer review scores and comments      |
| `olist_products_dataset.csv`             | Product attributes and dimensions        |
| `olist_sellers_dataset.csv`              | Sellers and their location               |
| `product_category_name_translation.csv`  | Portuguese → English category names      |

---

## Data Flow: Bronze → Silver → Gold

### 🥉 Bronze Layer: raw ingestion

**Script:** [`sql/Bronze_layer.sql`](sql/Bronze_layer.sql)

- A PL/pgSQL procedure drops and recreates one table per source file in the `bronze` schema, so every run is a **full reload**.
- The Airflow task `s3_to_postgres` lists every `.csv` under `data/`, downloads it, and appends it to `bronze.<file_name>` in **20,000-row chunks** with pandas `to_sql`. This keeps memory use flat for large files such as geolocation (~1M rows).
- Data is kept as close to the source as possible. Timestamps are stored as `text` and coordinates as `varchar`, and no cleaning is applied.

### 🥈 Silver Layer: cleaned and standardised

**Script:** [`sql/silver layer.sql`](sql/silver%20layer.sql) → procedure `silver.load_silver()`

Silver tables are created if missing, truncated, and reloaded from bronze on each run. They are truncated rather than dropped because the gold views depend on them. The transformations are:

| Table                               | Transformations                                                                   |
|-------------------------------------|-----------------------------------------------------------------------------------|
| `olist_customers_dataset`           | City names standardised with `INITCAP`                                            |
| `olist_geolocation_dataset`         | City names standardised with `INITCAP`                                            |
| `olist_sellers_dataset`             | City names standardised with `INITCAP`                                            |
| `olist_orders_dataset`              | All five lifecycle columns cast from `text` → `TIMESTAMP`                         |
| `olist_order_reviews_dataset`       | Review creation / answer timestamps cast from `text` → `DATE`                     |
| `olist_order_payments_dataset`      | Underscores in `payment_type` replaced with spaces (`credit_card` → `credit card`) |
| `product_category_name_translation` | Underscores in English category names replaced with spaces                        |
| `olist_products_dataset`            | Loaded as-is                                                                      |
| `olist_order_items_dataset`         | Loaded as-is                                                                      |

### 🥇 Gold Layer: business-ready snowflake schema

**Script:** [`sql/gold_layer.sql`](sql/gold_layer.sql)

The gold layer is a set of **views** over silver. Because they are views, they always reflect the latest silver data and need no reload. The model is a **snowflake schema**: the fact table links to customers and products, and sellers are reached through products.

```
                    ┌────────────────┐
                    │  dim_customer  │
                    │  customer_key  │
                    └───────▲────────┘
                            │
┌───────────────────────────┴──┐       ┌────────────────┐       ┌────────────────┐
│         fact_orders          │       │  dim_products  │       │  dim_sellers   │
│ one row per order item       │──────▶│  product_key   │──────▶│  seller_key    │
│ customer_key, product_key    │       │  seller_key    │       │                │
└──────────────────────────────┘       └────────────────┘       └────────────────┘
```

| View           | Type      | Grain                      | Description                                                                 |
|----------------|-----------|----------------------------|-----------------------------------------------------------------------------|
| `fact_orders`  | Fact      | One row per order item     | `customer_key`, `product_key`; order status and lifecycle timestamps; price, freight, total value; payment types, installments and allocated payment value; latest review score and comments |
| `dim_customer` | Dimension | One row per customer       | Surrogate `customer_key`, customer IDs, zip code, city, state, lat/lng      |
| `dim_products` | Dimension | One row per product–seller | Surrogate `product_key`, `seller_key`, English category name, name/description length, photo count, weight and dimensions |
| `dim_sellers`  | Dimension | One row per seller         | Surrogate `seller_key`, seller ID, zip code, city, state, lat/lng           |

Design notes:

- **Why `dim_products` is per product–seller:** in Olist, 1,225 products are sold by more than one seller (up to 8). Each row in `dim_products` has to point to a single seller, so each product/seller pair gets its own `product_key`. The pairs come from the order items, because the product catalog has no seller column.
- **Why the fact is per order item:** an order can contain several products. Payments and reviews are recorded per order, so they are aggregated per order before joining, and only the latest review is kept.
- **Summing payments:** `payment_value` is the order's payment split across its items by item value, so `SUM(payment_value)` doesn't double-count orders with several items. The 775 orders with no items (mostly canceled or unavailable) don't appear in the fact table.
- **Geolocation:** there are many rows per zip-code prefix, so it is reduced to one averaged lat/lng per prefix before joining. Joining it directly would duplicate customers and sellers.
- **Keys and names:** surrogate keys are generated with `ROW_NUMBER() OVER (ORDER BY <natural key>)`, and columns are renamed to business-friendly names (for example `customer_city` → `city`). Dimensions use `LEFT JOIN` so rows with no geolocation or category translation are kept.

Row counts on the full Olist dataset: `fact_orders` 112,650 · `dim_customer` 99,441 · `dim_products` 34,448 · `dim_sellers` 3,095.

---

## Airflow DAG

**DAG ID:** `s3_to_postgres_v010` · **Schedule:** `@daily` · **Retries:** 5 (5-minute delay)

```
check_for_new_files ──▶ register_procedures ──▶ run_bronze_layer ──▶ s3_to_postgres
        ──▶ run_silver_layer ──▶ run_gold_layer ──▶ archive_proceed_files_task
```

| Task                         | Operator                  | What it does                                                                  |
|------------------------------|---------------------------|-------------------------------------------------------------------------------|
| `check_for_new_files`        | `ShortCircuitOperator`    | Skips the run if `data/` has no CSVs, so an empty run can't wipe the warehouse |
| `register_procedures`        | `SQLExecuteQueryOperator` | Runs `Bronze_layer.sql` and `silver layer.sql` to create or update the procedures |
| `run_bronze_layer`           | `SQLExecuteQueryOperator` | `CALL bronze_layer_creation();` to recreate empty bronze tables               |
| `s3_to_postgres`             | `PythonOperator`          | Streams each CSV from MinIO into its bronze table in chunks and returns the loaded filenames via XCom |
| `run_silver_layer`           | `SQLExecuteQueryOperator` | `CALL silver.load_silver();`                                                  |
| `run_gold_layer`             | `SQLExecuteQueryOperator` | Runs `gold_layer.sql` to recreate the gold views                              |
| `archive_proceed_files_task` | `PythonOperator`          | Moves the loaded files from `data/` to `archive/` so they aren't loaded twice |

---

## Getting Started

### Prerequisites

- Docker Desktop
- A MinIO license file (the compose file mounts it from `C:/Users/ADMIN/minio/minio.license`; change this path for your machine)
- The Olist dataset from Kaggle

### 1. Configure environment

Create `medallion/.env`:

```env
AIRFLOW_UID=50000
FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

### 2. Start the services

```bash
cd medallion
docker compose up airflow-init
docker compose up -d
```

| Service       | URL                   | Default credentials |
|---------------|-----------------------|---------------------|
| Airflow UI    | http://localhost:8080 | `airflow / airflow` |
| MinIO console | http://localhost:9001 | set on first login  |
| PostgreSQL    | `localhost:5433`      | `airflow / airflow` |

### 3. Upload the source data

In the MinIO console, create the bucket `ecommercedataset` and upload the nine CSV files under the `data/` prefix.

### 4. Create Airflow connections

In **Admin → Connections**:

| Conn ID         | Type                | Settings                                                                                       |
|-----------------|---------------------|------------------------------------------------------------------------------------------------|
| `minio_conn`    | Amazon Web Services | Access key / secret key from MinIO; Extra: `{"endpoint_url": "http://minio:9000"}`             |
| `postgres_conn` | Postgres            | Host `postgres`, Port `5432`, Database `Retail_Store_DataWarehouse`, Login `airflow / airflow` |

### 5. Run the pipeline

Unpause and trigger `s3_to_postgres_v010` in the Airflow UI. A single run creates the schemas and procedures, loads **Bronze**, builds **Silver**, creates the **Gold** views, and archives the source files. You don't need to run any SQL by hand.

To load new data, upload fresh CSVs to `data/` again. The next scheduled run picks them up.

### Example queries

```sql
-- Orders and revenue by customer state (fact → customer)
SELECT c.state,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.payment_value)       AS revenue
FROM gold.fact_orders f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY revenue DESC;
```

```sql
-- Revenue by seller state and product category (fact → product → seller)
SELECT s.state          AS seller_state,
       p.category_name,
       COUNT(*)         AS items_sold,
       SUM(f.total_value) AS revenue
FROM gold.fact_orders f
JOIN gold.dim_products p ON f.product_key = p.product_key
JOIN gold.dim_sellers  s ON p.seller_key  = s.seller_key
GROUP BY s.state, p.category_name
ORDER BY revenue DESC
LIMIT 10;
```

---

## Roadmap

- [x] Orchestrate bronze → silver → gold end to end in one DAG
- [x] Model gold as a snowflake schema (fact → customer, fact → product → seller)
- [ ] Add data-quality checks between layers (row counts, null keys, duplicates)
- [ ] Add audit columns (`source_file`, `loaded_at`) to bronze tables

## Author

**Leslie Angu**, Lux Dev Data Engineering
