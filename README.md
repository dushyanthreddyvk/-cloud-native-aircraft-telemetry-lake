# Cloud-Native Aircraft Fleet Reliability Data Lake (GCP, Python)

This project simulates, cleans, stores, and analyzes high-volume aircraft sensor telemetry data to monitor real-time fleet health. Built using Python, Google Cloud Storage (GCS), and Google Cloud BigQuery.

## Architecture & Data Flow

1. **Simulation (`simulator.py`):** Generates synthetic flight logs for 500 distinct aircraft models. Injects exactly 15% corrupted/missing records (structural nulls, sensor NaNs, outliers).
2. **Raw Ingestion (`storage.py`):** Programmatically creates the GCS bucket (`aircraft-telemetry-lake`) and uploads raw files to partition directories: `gs://aircraft-telemetry-lake/raw/YYYY-MM-DD/`.
3. **ETL Processing (`etl.py`):** Standardizes timestamps, drops rows with missing structural fields, and cleans sensor readings by replacing physical outliers with NaN. Missing and outlier sensor fields are then imputed using a **rolling window average per aircraft group**.
4. **Data Warehouse Load (`warehouse.py`):** Creates the BigQuery dataset and table using optimized parameters:
   - **Time-Unit Partitioning:** Table partitioned daily by the `timestamp` column.
   - **Clustering:** Table clustered by `aircraft_id` and `aircraft_model`.
   Loads cleaned data from `gs://aircraft-telemetry-lake/processed/YYYY-MM-DD/` into the optimized BigQuery table.

## Local Mock Mode

If active GCP credentials are not available, the pipeline automatically runs in **Local Mock Mode**:
- **Mock GCS Storage:** Writes CSV files locally mimicking standard GCS structure in `./data/gcs_mock/`.
- **Mock BigQuery:** Simulates table creation, schema validation, data ingestion, and analytical SQL querying locally using **DuckDB** in `./data/gcs_mock/bq_mock.db`.

---

## Getting Started

### Prerequisites

- Python 3.9+
- Python virtual environment (recommended)

### Installation

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Verification Tests

To verify simulator corruption rate assertions, ETL cleaning logic, and mock database operations:
```bash
python3 verify_pipeline.py
```

### Run the Pipeline

#### 1. Running in Local Mock Mode (Offline)
Executes the full pipeline locally utilizing local folders and DuckDB:
```bash
python3 main.py --mock
```

#### 2. Running in GCP Cloud Mode (Active Connection)
Make sure you are authenticated with GCP:
```bash
gcloud auth application-default login
```
Set the project ID in your environment variables:
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export GCS_BUCKET_NAME="your-gcs-bucket-name"
```
Run the orchestrator:
```bash
python3 main.py --gcp
```

You can specify a target execution date (defaults to UTC today):
```bash
python3 main.py --mock --date 2026-07-17
```
