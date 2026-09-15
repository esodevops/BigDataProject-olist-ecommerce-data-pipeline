# Olist Azure Big Data Engineering Project

An end-to-end data engineering project built around the Brazilian Olist e-commerce dataset. The pipeline combines batch ingestion, cloud object storage, distributed transformation, NoSQL enrichment, serverless SQL serving, and downstream business intelligence.

The implementation uses Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, MongoDB, MySQL, PostgreSQL, Cloudflare R2 Object Storage and Azure Synapse Analytics. Supporting notebooks also demonstrate loading selected source data into PostgreSQL and MySQL.

![Project architecture](docs/Architecture%20Diagram.png)

## Table of contents

- [Architecture](#architecture)
- [Data flow](#data-flow)
- [Technology stack](#technology-stack)
- [Repository structure](#repository-structure)
- [Dataset](#dataset)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Running the project](#running-the-project)
- [Data transformations](#data-transformations)
- [Synapse serving layer](#synapse-serving-layer)
- [Security](#security)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)

## Architecture

```text
GitHub/HTTP and relational sources
                 |
                 v
        Azure Data Factory
                 |
                 v
       ADLS Gen2 — Bronze
                 |
                 v
         Azure Databricks <---- MongoDB product-category enrichment
                 |
                 v
       ADLS Gen2 — Silver (Delta/Parquet)
                 |
                 v
       Azure Synapse serverless SQL
                 |
                 v
       ADLS Gen2 — Gold / external table
                 |
                 v
      Power BI, Tableau, or Fabric
```

The architecture follows a medallion-style layout:

- **Bronze** contains raw CSV files ingested without business transformations.
- **Silver** contains cleaned, joined, enriched, and typed order-level data written by Databricks.
- **Gold** exposes filtered and serving-ready data through Synapse views and an external table.

## Data flow

1. Azure Data Factory iterates over a JSON list of source files.
2. Each CSV is copied from an HTTP source into the ADLS Gen2 `bronze/` directory.
3. Azure Databricks reads the Bronze CSV files with the ABFS driver.
4. Spark removes duplicate rows and fully empty rows.
5. Order timestamps are converted to dates.
6. Actual and estimated delivery durations are calculated, along with a delivery-delay flag.
7. Orders are joined with customers, payments, order items, products, and sellers.
8. Product-category translations are retrieved from MongoDB and joined to the Spark dataset.
9. The transformed dataset is written to the ADLS `silver/` location in Delta format.
10. Synapse serverless SQL reads the Silver files, creates Gold views, and materializes an external Gold table using CETAS.

## Technology stack

| Area | Technology | Purpose |
|---|---|---|
| Source data | Olist CSV dataset | E-commerce orders, customers, products, payments, reviews, and sellers |
| Orchestration | Azure Data Factory | Parameterized HTTP-to-ADLS ingestion with a ForEach activity |
| Data lake | Azure Data Lake Storage Gen2 | Bronze, Silver, and Gold storage layers |
| Processing | Azure Databricks and PySpark | Cleaning, transformation, joins, enrichment, and Delta output |
| NoSQL | MongoDB / MongoDB Atlas | Product-category translation enrichment |
| Relational examples | PostgreSQL and MySQL | Alternative ingestion targets for payments data |
| Serving | Azure Synapse Analytics serverless SQL | `OPENROWSET`, views, and external tables |
| Visualization | Power BI, Tableau, or Microsoft Fabric | Consumption of curated Gold data |
| Languages | Python, PySpark, SQL, JSON | Pipeline implementation and configuration |

## Repository structure

```text
.
├── data/                              # Local Olist CSV source files
├── docs/
│   ├── Architecture Diagram.png       # End-to-end architecture
│   ├── notes.txt                      # Reference links
│   └── sql code                       # Early Synapse SQL notes/examples
├── notebooks/
│   ├── CSVToMongoDBAtlas.ipynb        # Generic CSV-to-Atlas batch uploader
│   ├── DataIngestionToDB.ipynb        # PostgreSQL and MongoDB ingestion
│   ├── DataIngestionToDBMySQL.ipynb   # MySQL and MongoDB ingestion
│   └── databricks.ipynb               # Main Spark transformation pipeline
├── SnapseSQL/
│   ├── SQL-1 script.sql               # CSV query with OPENROWSET
│   ├── SQL-2 On OlistData.sql         # Silver Parquet query
│   ├── SQL-3 Create View.sql           # Gold base view
│   ├── SQL-4 View final 2.sql          # Delivered-orders view
│   ├── SQL-5 to gold layer.sql         # Gold external table with CETAS
│   └── SQL-final.sql                   # Final serving query
├── ForEachInput.json                  # ADF iteration input for GitHub paths
├── ForEachInputCloudflare.json        # ADF iteration input for object URLs
└── .env.example                       # Local configuration template
```

> The directory is named `SynapseSQL` in the repository. It contains Azure Synapse SQL scripts.

## Dataset

The project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

| File | Rows | Primary content |
|---|---:|---|
| `olist_customers_dataset.csv` | 99,441 | Customer identifiers and locations |
| `olist_geolocation_dataset.csv` | 19,015 | Postal-code coordinates and geography |
| `olist_order_items_dataset.csv` | 112,650 | Products, sellers, price, and freight per order item |
| `olist_order_payments_dataset.csv` | 103,886 | Payment methods, installments, and values |
| `olist_order_reviews_dataset.csv` | 99,224 | Review scores, comments, and timestamps |
| `olist_orders_dataset.csv` | 99,441 | Order status and lifecycle timestamps |
| `olist_products_dataset.csv` | 32,951 | Product category and physical attributes |
| `olist_sellers_dataset.csv` | 3,095 | Seller identifiers and locations |
| `product_category_name_translation.csv` | 71 | Portuguese-to-English category mapping |

The repository also contains `product_category_name_translation_atlas.csv`, an Atlas-oriented copy of the 71-row category translation table.

## Prerequisites

### Local ingestion notebooks

- Python 3.10 or newer
- JupyterLab, Jupyter Notebook, or VS Code notebook support
- Network access to the configured databases
- A PostgreSQL, MySQL, MongoDB, or MongoDB Atlas account as required by the selected notebook

Install the main local dependencies:

```bash
python -m pip install pandas python-dotenv psycopg2-binary mysql-connector-python pymongo dnspython certifi jupyter
```

### Azure pipeline

- An Azure subscription
- An ADLS Gen2 storage account with hierarchical namespace enabled
- An Azure Data Factory instance
- An Azure Databricks workspace and compatible compute
- A Microsoft Entra application/service principal or Unity Catalog storage credential
- An Azure Synapse Analytics workspace with serverless SQL
- Suitable Azure RBAC and data-plane permissions

For OAuth access, assign the service principal at least **Storage Blob Data Contributor** on the required storage account or container.

## Configuration

Copy the environment template and replace every placeholder:

```bash
cp .env.example .env
```

### PostgreSQL

```dotenv
hostnamePostgreSQL=your-postgresql-host
databasePostgreSQL=your-database
portPostgreSQL=5432
usernamePostgreSQL=your-username
passwordPostgreSQL=your-password
```

### MySQL

```dotenv
hostnameMySQL=your-mysql-host
databaseMySQL=your-database
portMySQL=3306
usernameMySQL=your-username
passwordMySQL=your-password
```

### Standard MongoDB

```dotenv
hostnameMongoDB=your-mongodb-host
databaseMongoDB=your-application-database
portMongoDB=27017
usernameMongoDB=your-username
passwordMongoDB=your-password
```

### MongoDB Atlas

```dotenv
hostnameMongoDBAtlas=cluster0.example.mongodb.net
databaseMongoDBAtlas=your-application-database
usernameMongoDBAtlas=your-atlas-database-user
passwordMongoDBAtlas=your-atlas-password
```

Use only the Atlas hostname in `hostnameMongoDBAtlas`; do not include `mongodb+srv://`, credentials, a path, or a port. The Atlas notebook constructs and URL-encodes the complete SRV URI.

Atlas also requires:

- A database user with `readWrite` permission on the selected application database.
- A Network Access entry that permits the client IP.
- A database name, not the Atlas project or cluster name.

### Azure Databricks storage authentication

The main Databricks notebook expects these values when using notebook-level OAuth configuration:

```dotenv
storage_account=your-adls-account-name
application_id=your-entra-application-client-id
directory_id=your-entra-tenant-id
secret_credentials=your-client-secret-value
```

For production, do not store the client secret in `.env`. Store it in a Databricks secret scope and retrieve it at runtime:

```python
secret_credentials = dbutils.secrets.get(
    scope="adls-secrets",
    key="db-client-secret",
)
```

The value saved under `db-client-secret` must be the Microsoft Entra client-secret **Value**, not the secret ID.

On serverless or other restricted compute, `spark.conf.set("fs.azure...")` can raise `CONFIG_NOT_AVAILABLE`. Use a Unity Catalog storage credential and external location instead. On compatible classic/dedicated compute, configure the OAuth properties in the cluster's Spark configuration using the `spark.hadoop.fs.azure...` prefix and restart the cluster.

## Running the project

### 1. Review the local source files

The CSV files are already available under `data/`. Verify that downloaded source files contain CSV data rather than an HTML download page:

```bash
head -n 2 data/olist_customers_dataset.csv
```

The first line should contain comma-separated column names and must not begin with `<!DOCTYPE html>`.

### 2. Populate MongoDB enrichment data

Open `notebooks/CSVToMongoDBAtlas.ipynb`. Its configurable defaults are:

```python
csv_path = Path("data/product_category_name_translation_atlas.csv")
collection_name = "product_categories"
batch_size = 1_000
```

Run the cells in order. The notebook:

1. Loads Atlas settings from `.env`.
2. Builds a TLS-enabled `mongodb+srv://` URI.
3. Tests connectivity with `ping`.
4. Converts pandas missing values to MongoDB-compatible `None` values.
5. Inserts records in batches.

Re-running the insert cell adds another copy of the records because it uses `insert_many`; clear the collection or implement a unique-key upsert when repeatable ingestion is required.

### 3. Configure ADF ingestion

Use one of the iteration manifests as the input to an ADF ForEach activity:

- `ForEachInput.json` contains source paths for GitHub-hosted files.
- `ForEachInputCloudflare.json` contains file paths relative to a Cloudflare R2 base URL.

Typical dynamic expressions inside the loop are:

```text
@item().csv_relative_url
@item().file_name
```

Configure the copy activity to write each file to:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/bronze/<file_name>
```

The ADF pipeline definition itself is not exported in this repository; recreate or import the linked services, datasets, pipeline, and trigger in your own environment.

### 4. Run Databricks transformations

Import `notebooks/databricks.ipynb` into Databricks and attach compatible compute. Update its storage account and container names for your environment.

The expected Bronze layout is:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/bronze/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_orders_dataset.csv
├── olist_products_dataset.csv
└── olist_sellers_dataset.csv
```

Run every cell in sequence. Before execution, replace any environment-specific hard-coded connection values with secret-backed configuration.

The Silver output is written to:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/silver/
```

### 5. Create the Synapse serving layer

Run the scripts under `SnapseSQL/` in numerical order using a Synapse serverless SQL database:

1. Validate file access with `SQL-1 script.sql` and `SQL-2 On OlistData.sql`.
2. Create the `gold` schema and base view with `SQL-3 Create View.sql`.
3. Create the delivered-orders view with `SQL-4 View final 2.sql`.
4. Create the external file format, data source, and Gold external table with `SQL-5 to gold layer.sql`.
5. Query the result using `SQL-final.sql`.

Replace all storage URLs, credential names, and database objects with values from your Azure environment. The identity used by Synapse must have access to the Silver and Gold storage paths.

## Data transformations

The main Spark pipeline performs the following transformations:

### Cleaning

```python
df.dropDuplicates().na.drop("all")
```

### Date conversion

The order purchase, approval, carrier delivery, customer delivery, and estimated delivery fields are converted to Spark date values.

### Delivery metrics

- `actual_delivery_time`: days from purchase to customer delivery
- `estimated_delivery_time`: days from purchase to estimated delivery
- `delay`: `1` when actual delivery exceeds the estimate, otherwise `0`

### Joins

```text
orders
  ├── customers       on customer_id
  ├── payments        on order_id
  ├── order_items     on order_id
  ├── products        on product_id
  ├── sellers         on seller_id
  └── category names  on product_category_name
```

All operational joins are left joins, retaining the order-side records when enrichment data is absent.

## Synapse serving layer

The Synapse scripts demonstrate:

- Querying lake files directly with `OPENROWSET`.
- Creating a `gold.final` view over Silver Parquet data.
- Creating `gold.final2` for delivered orders.
- Defining a Snappy-compressed Parquet external file format.
- Defining an ADLS-backed external data source.
- Using CETAS to write a serving-ready `gold.finaltable` external table.

The final query is:

```sql
SELECT *
FROM gold.finaltable;
```

## Security

- Never commit `.env`, client secrets, database passwords, access keys, connection strings, or Synapse master-key passwords.
- Keep `.env.example` limited to placeholders.
- Store Databricks credentials in secret scopes or Unity Catalog storage credentials.
- Prefer managed identities for ADLS access when available.
- Use least-privilege RBAC and database roles.
- Restrict MongoDB Atlas Network Access entries instead of allowing all IPs in production.
- Rotate any credential that has appeared in source code, notebook output, Git history, screenshots, or shared logs.
- Clear notebook outputs before committing because tracebacks can expose hosts, database names, and configuration details.


### Duplicate or ambiguous Spark columns

Join shared keys by name to retain a single join column:

```python
result = left_df.join(right_df, on="order_id", how="left")
```

