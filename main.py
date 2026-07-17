import argparse
import os
import logging
from datetime import datetime, timezone

import config
from simulator import DataSimulator
from storage import GCSStorageManager
from etl import TelemetryETLEngine
from warehouse import BigQueryWarehouseManager

logger = logging.getLogger("TelemetryOrchestrator")

def run_pipeline(execution_date_str: str, force_gcp: bool, force_mock: bool):
    """
    Orchestrates the entire end-to-end data engineering pipeline.
    """
    # 0. Override execution mode based on flags
    if force_gcp:
        config.MOCK_MODE = False
        os.environ["MOCK_MODE"] = "false"
        logger.info("Command line override: Forcing GCP CLOUD MODE.")
    elif force_mock:
        config.MOCK_MODE = True
        os.environ["MOCK_MODE"] = "true"
        logger.info("Command line override: Forcing LOCAL MOCK MODE.")
        
    logger.info(f"Pipeline Execution Date: {execution_date_str}")
    logger.info(f"Running pipeline. Mode: {'MOCK' if config.MOCK_MODE else 'GCP-CLOUD'}")
    
    # Parse date
    try:
        execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {execution_date_str}. Must be YYYY-MM-DD.")
        raise

    # 1. Telemetry Simulation (Phase 1)
    logger.info("=== STEP 1: RUNNING TELEMETRY SIMULATION ===")
    simulator = DataSimulator()
    # Run simulation setting the virtual clock to the execution date (at 00:00:00)
    raw_df = simulator.run_simulation(start_time=execution_date)
    
    # 2. Ingest Raw Data (Phase 2)
    logger.info("=== STEP 2: INGESTING RAW TELEMETRY TO STORAGE ===")
    storage_mgr = GCSStorageManager()
    
    # Create Raw GCS bucket / folder
    storage_mgr.create_bucket_if_not_exists(config.GCS_BUCKET_NAME)
    
    # Upload simulated raw data into Hive format directory structure
    raw_object_key = f"{config.RAW_PREFIX}/{execution_date_str}/telemetry_raw.csv"
    raw_uri = storage_mgr.upload_dataframe(
        df=raw_df,
        bucket_name=config.GCS_BUCKET_NAME,
        object_key=raw_object_key,
        file_format="csv"
    )
    
    # 3. ETL Transformation Engine (Phase 3)
    logger.info("=== STEP 3: EXECUTING ETL ENGINE ===")
    # Download raw data to mimic pull-based ETL from raw zone
    raw_pulled_df = storage_mgr.download_dataframe(
        bucket_name=config.GCS_BUCKET_NAME,
        object_key=raw_object_key,
        file_format="csv"
    )
    
    etl_engine = TelemetryETLEngine()
    cleaned_df = etl_engine.clean_telemetry(raw_pulled_df)
    
    # Upload clean processed data into Hive format directory structure
    processed_object_key = f"{config.PROCESSED_PREFIX}/{execution_date_str}/telemetry_processed.csv"
    processed_uri = storage_mgr.upload_dataframe(
        df=cleaned_df,
        bucket_name=config.GCS_BUCKET_NAME,
        object_key=processed_object_key,
        file_format="csv"
    )
    
    # 4. Data Warehousing & Optimization (Phase 4)
    logger.info("=== STEP 4: LOADING TO OPTIMIZED BIGQUERY WAREHOUSE ===")
    warehouse_mgr = BigQueryWarehouseManager()
    
    # Create Dataset and partitioned/clustered table
    table_fullname = warehouse_mgr.create_dataset_and_table()
    
    # Load processed GCS data into BigQuery
    rows_loaded = warehouse_mgr.load_data_from_gcs(source_uri=processed_uri)
    logger.info(f"Loaded {rows_loaded} rows into {table_fullname}.")

    # 5. Run Analytical/Reliability Queries (Verification)
    logger.info("=== STEP 5: RUNNING FLEET HEALTH ANALYTICAL QUERIES ===")
    
    # Query 1: fleet stats summary
    fleet_stats_query = f"""
    SELECT 
        aircraft_model,
        COUNT(DISTINCT aircraft_id) as active_aircraft_count,
        ROUND(AVG(engine_temperature), 2) as avg_engine_temp_c,
        ROUND(AVG(vibration_level), 2) as avg_vibration_index,
        ROUND(AVG(fuel_flow_rate), 2) as avg_fuel_flow_kgh,
        COUNT(error_code) as errors_logged
    FROM {config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}
    GROUP BY aircraft_model
    ORDER BY avg_vibration_index DESC;
    """
    
    stats_df = warehouse_mgr.query_table(fleet_stats_query)
    print("\nFleet Reliability Summary Metrics:")
    print(stats_df.to_string(index=False))
    
    # Query 2: top anomalies/errors logged
    error_summary_query = f"""
    SELECT 
        error_code,
        COUNT(*) as occurrence_count,
        ROUND(AVG(engine_temperature), 2) as avg_temp_when_failed,
        ROUND(AVG(vibration_level), 2) as avg_vibration_when_failed
    FROM {config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}
    WHERE error_code IS NOT NULL
    GROUP BY error_code
    ORDER BY occurrence_count DESC;
    """
    
    error_df = warehouse_mgr.query_table(error_summary_query)
    print("\nSimulated Critical Fault Code Statistics:")
    if not error_df.empty:
        print(error_df.to_string(index=False))
    else:
        print("No error codes logged in BQ.")
        
    # Clean up BQ connection
    warehouse_mgr.close()
    
    logger.info("=== DATA LAKE PIPELINE RUN COMPLETE ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cloud-Native Aircraft Fleet Reliability Data Lake Ingestion & ETL Orchestrator")
    parser.add_argument(
        "--date", 
        type=str, 
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Pipeline execution date in YYYY-MM-DD format (default: today)"
    )
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--gcp", 
        action="store_true",
        help="Force execution in active GCP mode (uses real GCS & BigQuery services)"
    )
    mode_group.add_argument(
        "--mock", 
        action="store_true",
        help="Force execution in Local Mock Mode (runs offline using local directories & DuckDB)"
    )

    args = parser.parse_args()
    
    run_pipeline(
        execution_date_str=args.date,
        force_gcp=args.gcp,
        force_mock=args.mock
    )
