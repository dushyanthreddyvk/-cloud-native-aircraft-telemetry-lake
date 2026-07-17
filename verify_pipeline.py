import os
import logging
import pandas as pd
import numpy as np

import config
from simulator import DataSimulator
from etl import TelemetryETLEngine
from warehouse import BigQueryWarehouseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PipelineVerifier")

def test_simulator_corruption_rate():
    """
    Verifies that the simulator injects exactly the specified corruption rate (15%).
    """
    logger.info("--- Testing Simulator Corruption Injection ---")
    simulator = DataSimulator()
    clean_df = simulator.generate_clean_telemetry(start_time=pd.Timestamp.now())
    total_records = len(clean_df)
    
    # Run the corruption injection
    corrupted_df = simulator.inject_corruption(clean_df.copy())
    
    # Identify corrupted rows
    # A row is corrupted if it has structural nulls, sensor NaNs, sensor outliers, or a fault error code.
    
    # 1. Structural nulls
    is_structural_null = corrupted_df["timestamp"].isna() | corrupted_df["aircraft_id"].isna()
    
    # 2. Sensor NaNs
    sensor_cols = ["engine_temperature", "altitude", "fuel_flow_rate", "vibration_level"]
    is_sensor_nan = corrupted_df[sensor_cols].isna().any(axis=1)
    
    # 3. Sensor Outliers
    is_outlier = pd.Series([False] * len(corrupted_df))
    for sensor, limits in config.SENSOR_BOUNDARIES.items():
        min_val = limits["min"]
        max_val = limits["max"]
        # Outliers exist if the value is not NaN but lies outside bounds
        is_outlier |= (
            corrupted_df[sensor].notna() & 
            ((corrupted_df[sensor] < min_val) | (corrupted_df[sensor] > max_val))
        )
        
    # 4. Fault Code (error_code is set)
    is_fault = corrupted_df["error_code"].notna()
    
    # Total corrupted rows is the union of all four conditions
    is_corrupted = is_structural_null | is_sensor_nan | is_outlier | is_fault
    total_corrupted_count = is_corrupted.sum()
    
    calculated_rate = total_corrupted_count / total_records
    expected_count = int(total_records * config.CORRUPTION_RATE)
    
    logger.info(f"Total Records: {total_records}")
    logger.info(f"Corrupted Records Count: {total_corrupted_count} (Expected: {expected_count})")
    logger.info(f"Calculated Corruption Rate: {calculated_rate * 100:.2f}% (Expected: {config.CORRUPTION_RATE * 100}%)")
    
    assert total_corrupted_count == expected_count, (
        f"Corruption count mismatch: got {total_corrupted_count}, expected {expected_count}"
    )
    logger.info("PASS: Simulator corruption injection matches exact requirements.")

def test_etl_cleaning_and_imputation():
    """
    Verifies that the TelemetryETLEngine cleans structural nulls,
    imputes missing data using rolling averages, and handles outliers.
    """
    logger.info("--- Testing ETL Cleaning and Imputation ---")
    simulator = DataSimulator()
    raw_df = simulator.run_simulation()
    
    etl = TelemetryETLEngine()
    cleaned_df = etl.clean_telemetry(raw_df)
    
    # Assertions
    # 1. Structural fields must not contain nulls
    assert cleaned_df["timestamp"].isna().sum() == 0, "ETL failed to drop null timestamps"
    assert cleaned_df["aircraft_id"].isna().sum() == 0, "ETL failed to drop null aircraft_ids"
    
    # 2. Sensor columns must not contain NaNs (all imputed)
    sensor_cols = ["engine_temperature", "altitude", "fuel_flow_rate", "vibration_level"]
    for col in sensor_cols:
        nan_count = cleaned_df[col].isna().sum()
        assert nan_count == 0, f"ETL failed to impute all NaNs in sensor: {col} ({nan_count} remaining)"
        
    # 3. No sensor outliers should remain (all replaced by imputed values)
    for col, limits in config.SENSOR_BOUNDARIES.items():
        min_val = limits["min"]
        max_val = limits["max"]
        outlier_count = ((cleaned_df[col] < min_val) | (cleaned_df[col] > max_val)).sum()
        assert outlier_count == 0, f"ETL failed to remove sensor outliers in: {col} ({outlier_count} remaining)"
        
    # 4. Check that data types are correct
    assert cleaned_df["engine_temperature"].dtype == float
    assert cleaned_df["altitude"].dtype == float
    assert cleaned_df["fuel_flow_rate"].dtype == float
    assert cleaned_df["vibration_level"].dtype == float
    
    # 5. Check timestamp format (ISO 8601 YYYY-MM-DDTHH:MM:SSZ)
    sample_timestamp = cleaned_df["timestamp"].iloc[0]
    assert len(sample_timestamp) == 20 and sample_timestamp.endswith("Z"), (
        f"Timestamp format is not ISO 8601 UTC: {sample_timestamp}"
    )
    
    logger.info("PASS: ETL cleaning, outlier filtering, and imputation succeeded.")

def test_mock_warehouse_operations():
    """
    Verifies that the DuckDB mock database operates correctly.
    """
    logger.info("--- Testing Mock Warehouse DDL and Loading ---")
    warehouse = BigQueryWarehouseManager(mock_mode=True)
    
    # Create Dataset/Table
    table_name = warehouse.create_dataset_and_table()
    assert table_name == f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"
    
    # Insert mock row
    insert_sql = f"""
    INSERT INTO {config.BQ_DATASET_ID}.{config.BQ_TABLE_ID} VALUES
    ('2026-07-17 12:00:00', 'AC001', 'Boeing 737-800', 720.5, 35000.0, 3100.0, 1.4, NULL);
    """
    warehouse.con.execute(insert_sql)
    
    # Verify insert
    res = warehouse.con.execute(f"SELECT COUNT(*) FROM {config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}").fetchone()
    assert res[0] == 1, "Mock DB failed to insert record"
    
    # Verify schema
    desc_df = warehouse.con.execute(f"DESCRIBE {config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}").df()
    columns = desc_df["column_name"].tolist()
    expected_cols = [
        "timestamp", "aircraft_id", "aircraft_model", 
        "engine_temperature", "altitude", "fuel_flow_rate", 
        "vibration_level", "error_code"
    ]
    for col in expected_cols:
        assert col in columns, f"Expected column {col} not found in mock schema"
        
    warehouse.close()
    logger.info("PASS: Mock BigQuery warehouse and schema initialized successfully.")

if __name__ == "__main__":
    logger.info("=== STARTING PIPELINE VALIDATION TESTS ===")
    test_simulator_corruption_rate()
    test_etl_cleaning_and_imputation()
    test_mock_warehouse_operations()
    logger.info("=== ALL PIPELINE VALIDATION TESTS PASSED ===")
