# Data Engineering Project

Two-part data engineering pipeline built with Python, Pandas, Airflow, and Azure Blob Storage.

| Part | Topic | Source |
|------|-------|--------|
| 1 | Batch Processing | NYC Yellow Taxi trip data (Jan 2025, 3.5 M rows) |
| 2 | Real-Time Data Processing | Amazon Sales data (CSV, file-triggered) |

---

## Project Structure

```
.
├── requirements.txt
├── batch_processing/
│   ├── pipeline.py                  # Reader → Validator → Processor → BackupValidator → Writer
│   ├── input/                       # place yellow_tripdata_2025-01.parquet here
│   ├── output/                      # processed parquet + report written here
│   └── dags/
│       └── taxi_pipeline_dag.py     # Airflow DAG (scheduled / manual trigger)
└── real_time_processing/
    ├── pipeline_rt.py               # Reader → Validator → Processor → BackupValidator → Writer
    ├── input/                       # DROP .csv or .xlsx files here (amazon.csv included)
    ├── output/                      # processed CSV + report + rejection log written here
    ├── processed/                   # source files archived here after processing
    └── dags/
        └── rt_pipeline_dag.py       # Airflow DAG with PythonSensor (polls every 30 s)
```

---

## Setup

### Prerequisites

- Python 3.11+
- Apache Airflow 3.x (for DAG execution)
- An Azure Storage account (optional — pipelines run fully locally without it)

### Install dependencies

```bash
pip install -r requirements.txt
```

### Azure Blob Storage (optional)

Set the following environment variables to enable cloud upload:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
export AZURE_CONTAINER="your-container-name"
```

If these variables are not set, both pipelines write output **locally only** and log a warning — they do not crash.

---

## Part 1 — Batch Processing

### Data

Download the source file from the NYC TLC website and place it in `batch_processing/`:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

```bash
curl -L -o batch_processing/input/yellow_tripdata_2025-01.parquet \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
```

### Run directly

```bash
cd batch_processing
python pipeline.py
```

### Run via Airflow

```bash
# Symlink the DAG file into Airflow's dags folder (one-time setup)
ln -sf "$(pwd)/batch_processing/dags/taxi_pipeline_dag.py" ~/airflow/dags/taxi_pipeline_dag.py

airflow dags reserialize
airflow dags trigger nyc_taxi_pipeline
```

### What it does

| Stage | Description |
|-------|-------------|
| Reader | Reads `yellow_tripdata_2025-01.parquet` |
| Validator | Checks mandatory/non-mandatory columns against per-column rules; fatal on missing column |
| Processor | Drops invalid rows (including out-of-month trips), fills optional nulls, derives 11 new columns |
| BackupValidator | Confirms derived columns exist and no impossible values survived |
| Writer | Writes processed parquet + report locally; uploads to Azure with 3-retry back-off |

### Derived columns added

`trip_duration_minutes` · `average_speed_mph` · `pickup_year` · `pickup_month` · `pickup_hour` · `pickup_day_of_week` · `pickup_date` · `revenue_per_mile` · `trip_distance_category` · `fare_category` · `trip_time_of_day`

### Dropped columns

`VendorID` · `store_and_fwd_flag` · `RatecodeID`

---

## Part 2 — Real-Time Data Processing

### Dataset

Real Amazon product and review data (`input/amazon.csv`) — 1,465 rows, 16 columns.  
Source: [Amazon Sales Dataset on Kaggle](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset)

The dataset contains genuine data quality issues: prices stored as strings with `₹` symbols and comma-separated thousands, discount percentages with `%` signs, 114 duplicate product IDs, and 1 unparseable rating.

### Run directly

```bash
cd real_time_processing
python3 pipeline_rt.py input/amazon.csv
```

### Run via Airflow (file-triggered)

```bash
# Symlink the DAG file into Airflow's dags folder (one-time setup)
ln -sf "$(pwd)/real_time_processing/dags/rt_pipeline_dag.py" ~/airflow/dags/rt_pipeline_dag.py

airflow dags reserialize
airflow dags unpause rt_ecommerce_pipeline
```

Then drop any `.csv` or `.xlsx` file into `real_time_processing/input/`.  
The Airflow sensor detects it within **30 seconds** and runs the full pipeline automatically.

### What it does

| Stage | Description |
|-------|-------------|
| Sensor | `PythonSensor` polls `input/` every 30 s; unsupported/empty files moved to `input/rejected/` |
| Reader | Auto-detects `.csv` (with encoding fallback) or `.xlsx`; raises on empty/corrupt files |
| Validator | Checks mandatory columns, nulls, ASIN format, price parseability, rating range |
| Processor | Deduplicates on `product_id`; strips `₹`/`%` symbols; drops invalid rows; derives 4 columns |
| BackupValidator | Sanity checks including 0-row case (pipeline does not crash on empty result) |
| Writer | Writes processed CSV + report + **rejection log** locally; uploads to Azure (3 retries) |
| Archive | Moves source file to `processed/` so the next file can be picked up |

### Derived columns added

`top_category` · `discount_amount` · `price_tier` · `rating_band`

### Outputs per file

| File | Description |
|------|-------------|
| `<name>_processed.csv` | Cleaned, enriched data |
| `<name>_report.txt` | Summary statistics + data quality score |
| `<name>_rejected.csv` | Every dropped row with `rejection_reason` and `rejected_at_step` |

### Robustness / foolproof features

| Scenario | Behaviour |
|----------|-----------|
| `.txt`, `.pdf`, `.json`, etc. dropped in `input/` | Moved to `input/rejected/` with reason file; pipeline keeps running |
| 0-byte file | Same as above |
| CSV with wrong/missing columns | Fatal validation error — clear message, no crash |
| All rows invalid (0 valid after cleaning) | Empty output file written; report flags 0 rows; no crash |
| Azure credentials missing | Warning logged; local output still written |
| Azure upload fails | Retried 3× with exponential back-off (2 s, 4 s); then warning — no crash |
| Multiple files in `input/` at once | Processed one at a time (`max_active_runs=1`); next run picks up the next file |
| File dropped mid-run | Stays in `input/`; picked up by the next DAG run |

---

## Data Quality Score

Both pipelines report a **data quality score** in the report file:

```
Data quality score   : 92.2%
```

Calculated as: `(valid rows written / total rows read) × 100`

---

## Validation Rules

### Part 1 — NYC Taxi

| Column | Mandatory | Rule |
|--------|-----------|------|
| `tpep_pickup_datetime` | Yes | Non-null datetime |
| `tpep_dropoff_datetime` | Yes | Non-null, must be > pickup |
| `passenger_count` | Yes | Integer 1–9 |
| `trip_distance` | Yes | 0.01–200 miles |
| `PULocationID` / `DOLocationID` | Yes | TLC zone 1–265 |
| `payment_type` | Yes | {0,1,2,3,4,5,6} |
| `fare_amount` / `total_amount` | Yes | ≥ 0 |
| `tip_amount`, `tolls_amount`, fees | No | ≥ 0 when present |
| `store_and_fwd_flag` | No | {Y, N} |
| `RatecodeID` | No | {1–6} |

### Part 2 — Amazon Sales

| Column | Mandatory | Rule |
|--------|-----------|------|
| `product_id` | Yes | Non-null, unique, regex `^B[A-Z0-9]{9}$` (Amazon ASIN) |
| `product_name` | Yes | Non-null |
| `category` | Yes | Non-null pipe-separated hierarchy |
| `discounted_price` | Yes | Non-null, parseable after stripping `₹` and commas, > 0 |
| `actual_price` | Yes | Non-null, parseable after stripping `₹` and commas, > 0 |
| `discount_percentage` | Yes | Non-null, parseable after stripping `%` |
| `rating` | Yes | Non-null, parseable, 1.0–5.0 |
| `rating_count` | No | Parseable after stripping commas; nulls filled with 0 |

---

## Environment Variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `AZURE_STORAGE_CONNECTION_STRING` | Both pipelines | Azure Storage connection string |
| `AZURE_CONTAINER` | Both pipelines | Target blob container name |

---

## Deadline

**Submission:** 3 May 2026 23:59  
**Defence:** 4 May 2026 08:00
