# 🐘 Walmart Neon DB Postgres Setup Guide
### Source OLTP Database Provisioning, Schema Modeling & Data Ingestion

[![Back to Main Overview](https://img.shields.io/badge/⬅_Back_to-Main_Overview-lightgrey?style=for-the-badge)](../README.md)
[![Next: Airflow & dbt Guide](https://img.shields.io/badge/Next_Guide-Airflow,_dbt_&_Docker_Setup_➔-017CEE?style=for-the-badge)](../databricks-dbt-project/README.md)

---

## 📖 Overview

In this pipeline, **Neon Serverless Postgres** acts as the transactional (OLTP) source database representing Walmart's operational store and order management system. 

This guide walks you through:
1. Creating a cloud serverless Postgres database on Neon.
2. Executing the relational DDL (`raw` schema).
3. Bulk loading 6 core retail datasets using high-throughput Python COPY streaming.
4. Verifying data integrity before Databricks CDC ingestion begins.

---

## 🛠️ Prerequisites

- **Neon Account**: Sign up for free at [neon.tech](https://neon.tech).
- **Python 3.10+** installed locally.
- **Dependencies**: Install required packages:
  ```bash
  pip install psycopg2-binary python-dotenv
  ```
  *(Or if using uv / pyproject.toml in this directory):*
  ```bash
  uv sync
  ```

---

## 🚀 Step-by-Step Setup

### Step 1: Create a Neon Project

1. Log into your [Neon Console](https://console.neon.tech).
2. Click **Create Project**.
3. Name your project: `walmart-de-project`.
4. Choose your cloud provider and region (e.g., `AWS us-west-2`).
5. Set Database name to: `walmart_db`.
6. Once created, copy the connection string under **Connection Details**:
   ```text
   postgresql://neondb_owner:<PASSWORD>@<HOST>/walmart_db?sslmode=require
   ```

---

### Step 2: Configure Local Environment Variables

1. In this directory, copy the template file [.env.example](file:///d:/DE_Projects/walmart-de-project-dbt/Walmart%20Neon%20DB%20Postgres%20Setup/.env.example) to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and paste your actual connection string and Neon API key:
   ```ini
   POSTGRES_CONN=postgresql://neondb_owner:YOUR_PASSWORD@ep-dark-mountain-armwsg9l.c-4.us-west-2.aws.neon.tech/walmart_db?sslmode=require
   NEON_API_KEY=napi_YOUR_NEON_API_KEY
   ```
   > [!NOTE]
   > The `.env` file is strictly ignored by Git to protect your database credentials.

---

### Step 3: Source Relational Data Model & ER Diagram

Before running the DDL, review the Entity-Relationship (ER) model below. It defines the 6 normalized retail entities and their foreign key constraints:

![Walmart Relational ER Diagram](../assets/walmart-er-diagram.png)

```mermaid
erDiagram
    CUSTOMERS ||--o{ ORDERS : "places"
    STORES ||--o{ ORDERS : "fulfills"
    STORES ||--o{ EMPLOYEES : "employs"
    ORDERS ||--|{ ORDER_ITEMS : "contains"
    PRODUCTS ||--o{ ORDER_ITEMS : "ordered in"

    CUSTOMERS {
        bigint customer_id PK
        varchar first_name
        varchar last_name
        varchar email
        varchar phone
        varchar city
        varchar province
        varchar country
        timestamp created_timestamp
        timestamp updated_timestamp
        char is_active
    }

    STORES {
        bigint store_id PK
        varchar store_name
        varchar city
        varchar province
        varchar country
        timestamp created_timestamp
        timestamp updated_timestamp
        char is_active
    }

    EMPLOYEES {
        bigint employee_id PK
        bigint store_id FK
        varchar first_name
        varchar last_name
        varchar email
        varchar job_title
        numeric salary
        timestamp created_timestamp
        timestamp updated_timestamp
        char is_active
    }

    PRODUCTS {
        bigint product_id PK
        varchar product_name
        varchar category
        varchar brand
        numeric price
        timestamp created_timestamp
        timestamp updated_timestamp
        char is_active
    }

    ORDERS {
        bigint order_id PK
        bigint customer_id FK
        bigint store_id FK
        timestamp order_timestamp
        varchar payment_method
        varchar order_status
        numeric total_amount
        timestamp created_timestamp
        timestamp updated_timestamp
        char is_active
    }

    ORDER_ITEMS {
        bigint order_item_id PK
        bigint order_id FK
        bigint product_id FK
        int quantity
        numeric unit_price
        numeric line_amount
        timestamp created_timestamp
        timestamp updated_timestamp
        char is_active
    }
```

---

### Step 4: Run the Schema DDL

The database tables are organized under a dedicated `raw` schema. You can run the DDL in the **Neon SQL Editor** or via `psql`.

Open [walmart_schema.sql](file:///d:/DE_Projects/walmart-de-project-dbt/datasets/ddl/walmart_schema.sql) and execute the SQL:

```sql
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    city VARCHAR(100),
    province VARCHAR(100),
    country VARCHAR(100),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active CHAR(1)
);

CREATE TABLE stores (
    store_id BIGINT PRIMARY KEY,
    store_name VARCHAR(255),
    city VARCHAR(100),
    province VARCHAR(100),
    country VARCHAR(100),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active CHAR(1)
);

CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    price NUMERIC(10,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active CHAR(1)
);

CREATE TABLE employees (
    employee_id BIGINT PRIMARY KEY,
    store_id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    job_title VARCHAR(100),
    salary NUMERIC(10,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active CHAR(1)
);

CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    store_id BIGINT,
    order_timestamp TIMESTAMP,
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    total_amount NUMERIC(12,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active CHAR(1)
);

CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price NUMERIC(10,2),
    line_amount NUMERIC(12,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active CHAR(1)
);
```

---

### Step 5: Ingest the Datasets

To seed your database with historical transactions, run the [load_data.py](file:///d:/DE_Projects/walmart-de-project-dbt/datasets/load_data.py) script.

1. Ensure your connection string is set in `datasets/load_data.py` or injected via environment variable:
   ```python
   conn_string = os.getenv("POSTGRES_CONN", "your_connection_string_here")
   ```
2. Run the loader from the repository root:
   ```bash
   cd datasets
   python load_data.py
   ```

The script streams the CSV files directly to Neon Postgres using `copy_expert`:
```text
Loading customers.csv into raw.customers...
✓ Successfully loaded customers.csv
Loading stores.csv into raw.stores...
✓ Successfully loaded stores.csv
Loading products.csv into raw.products...
✓ Successfully loaded products.csv
Loading employees.csv into raw.employees...
✓ Successfully loaded employees.csv
Loading orders.csv into raw.orders...
✓ Successfully loaded orders.csv
Loading order_items.csv into raw.order_items...
✓ Successfully loaded order_items.csv

✓ All data loaded successfully!
```

---

### Step 5: Verify Data Integrity

Run the following validation query in the Neon SQL Editor to verify row counts:

```sql
SELECT 'customers' AS table_name, count(*) AS total_rows FROM raw.customers
UNION ALL
SELECT 'stores', count(*) FROM raw.stores
UNION ALL
SELECT 'products', count(*) FROM raw.products
UNION ALL
SELECT 'employees', count(*) FROM raw.employees
UNION ALL
SELECT 'orders', count(*) FROM raw.orders
UNION ALL
SELECT 'order_items', count(*) FROM raw.order_items;
```

Expected row counts:
- `customers`: ~2,000 rows
- `stores`: ~50 rows
- `products`: ~500 rows
- `employees`: ~500 rows
- `orders`: ~15,000 rows
- `order_items`: ~45,000 rows

---

### Step 6: Connecting Neon to Databricks for CDC

In the next phase, Databricks connects to this Neon Postgres instance to replicate these tables into the **Bronze Layer** (`walmart.bronze.*`).

Store the Neon connection safely inside Databricks using the Databricks CLI:
```bash
# 1. Create a secret scope
databricks secrets create-scope --scope neon_scope

# 2. Store the Neon Postgres connection string securely
databricks secrets put-secret --scope neon_scope --key postgres_conn
```

In your Databricks ingestion notebook / job, reference this secret directly:
```python
postgres_conn = dbutils.secrets.get(scope="neon_scope", key="postgres_conn")
```

---

## 🎯 Next Step

Now that your source OLTP database is initialized with data, proceed to spin up the Airflow orchestration cluster and configure the dbt models:

[![Proceed to Airflow, dbt & Docker Setup Guide](https://img.shields.io/badge/Next_Step-Airflow,_dbt_&_Docker_Setup_Guide-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](../databricks-dbt-project/README.md)
