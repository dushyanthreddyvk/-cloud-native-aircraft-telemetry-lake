import os
import logging
import pandas as pd
from typing import List, Optional

# Conditional import for bigquery
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
except ImportError:
    bigquery = None
    NotFound = Exception

# Import duckdb for SQL mocking
import duckdb

import config

logger = logging.getLogger("BigQueryWarehouseManager")

class BigQueryWarehouseManager:
    """
    Manages the Google Cloud BigQuery schema, table creation, and data loading processes.
    Defines daily time-unit partitioning on 'timestamp' and clustering on 'aircraft_id' and 'aircraft_model'.
    Includes a Local Mock Mode utilizing DuckDB to verify DDL and DML operations.
    """
    
    def __init__(self, mock_mode: bool = config.MOCK_MODE):
        self.mock_mode = mock_mode
        self.client = None
        self.dataset_id = config.BQ_DATASET_ID
        self.table_id = config.BQ_TABLE_ID
        
        if self.mock_mode:
            logger.info("BigQueryWarehouseManager running in LOCAL MOCK MODE (DuckDB).")
            self.db_path = config.LOCAL_DB_PATH
            # Establish DuckDB connection
            self.con = duckdb.connect(database=self.db_path)
            self._init_mock_db()
        else:
            logger.info("BigQueryWarehouseManager running in GCP CLOUD MODE.")
            if bigquery is None:
                raise ImportError("google-cloud-bigquery library is missing or cannot be imported.")
            self.client = bigquery.Client(project=config.GCP_PROJECT_ID)

    def _init_mock_db(self):
        """Initializes the mock database tables using DuckDB."""
        # Create schema mimicking GCS dataset grouping
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dataset_id};")
        # Define table structure in DuckDB
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.dataset_id}.{self.table_id} (
            timestamp TIMESTAMP NOT NULL,
            aircraft_id VARCHAR NOT NULL,
            aircraft_model VARCHAR NOT NULL,
            engine_temperature DOUBLE NOT NULL,
            altitude DOUBLE NOT NULL,
            fuel_flow_rate DOUBLE NOT NULL,
            vibration_level DOUBLE NOT NULL,
            error_code VARCHAR
        );
        """
        self.con.execute(create_sql)
        logger.info(f"[MOCK] Initialized DuckDB table: {self.dataset_id}.{self.table_id}")

    def create_dataset_and_table(self) -> str:
        """
        Creates BigQuery Dataset and Table with partitioning and clustering enabled.
        Returns the full target table URI or name.
        """
        if self.mock_mode:
            # Re-run initialization to verify table structure
            self._init_mock_db()
            mock_ddl = f"""
            -- SIMULATED BIGQUERY DDL
            CREATE SCHEMA IF NOT EXISTS `{config.GCP_PROJECT_ID}.{self.dataset_id}`;
            
            CREATE TABLE IF NOT EXISTS `{config.GCP_PROJECT_ID}.{self.dataset_id}.{self.table_id}` (
                timestamp TIMESTAMP REQUIRED,
                aircraft_id STRING REQUIRED,
                aircraft_model STRING REQUIRED,
                engine_temperature FLOAT64 REQUIRED,
                altitude FLOAT64 REQUIRED,
                fuel_flow_rate FLOAT64 REQUIRED,
                vibration_level FLOAT64 REQUIRED,
                error_code STRING
            )
            PARTITION BY DATE(timestamp)
            CLUSTER BY aircraft_id, aircraft_model;
            """
            logger.info(f"[MOCK] Simulated BigQuery DDL:\n{mock_ddl}")
            return f"{config.GCP_PROJECT_ID}.{self.dataset_id}.{self.table_id}"

        # Real GCP Mode execution
        dataset_ref = bigquery.DatasetReference(config.GCP_PROJECT_ID, self.dataset_id)
        
        # 1. Create dataset if not exists
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"BigQuery Dataset {self.dataset_id} already exists.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = config.LOCATION
            dataset.description = "Aircraft fleet telemetry data lake analytics warehouse."
            self.client.create_dataset(dataset)
            logger.info(f"Created BigQuery Dataset: {self.dataset_id} in {config.LOCATION}")

        # 2. Define Table with Schema, Partitioning, and Clustering
        table_ref = dataset_ref.table(self.table_id)
        
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="UTC standard log timestamp"),
            bigquery.SchemaField("aircraft_id", "STRING", mode="REQUIRED", description="Aircraft unique tail number"),
            bigquery.SchemaField("aircraft_model", "STRING", mode="REQUIRED", description="Aircraft model/type"),
            bigquery.SchemaField("engine_temperature", "FLOAT", mode="REQUIRED", description="Engine temperature in Celsius"),
            bigquery.SchemaField("altitude", "FLOAT", mode="REQUIRED", description="Flight altitude in feet"),
            bigquery.SchemaField("fuel_flow_rate", "FLOAT", mode="REQUIRED", description="Fuel consumption rate in kg/h"),
            bigquery.SchemaField("vibration_level", "FLOAT", mode="REQUIRED", description="Engine vibration index"),
            bigquery.SchemaField("error_code", "STRING", mode="NULLABLE", description="Active fault diagnostics code")
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        
        # Highly Important Optimization 1: Partition by 'timestamp' (daily)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        
        # Highly Important Optimization 2: Cluster by 'aircraft_id' and 'aircraft_model'
        table.clustering_fields = ["aircraft_id", "aircraft_model"]
        
        # 3. Create table if not exists
        try:
            self.client.get_table(table_ref)
            logger.info(f"BigQuery Table {self.dataset_id}.{self.table_id} already exists.")
        except NotFound:
            self.client.create_table(table)
            logger.info(f"Created Optimized BigQuery Table: {self.dataset_id}.{self.table_id}")
            logger.info("- Time-unit Partitioning Field: 'timestamp' (DAY)")
            logger.info("- Clustering Fields: ['aircraft_id', 'aircraft_model']")
            
        return f"{config.GCP_PROJECT_ID}.{self.dataset_id}.{self.table_id}"

    def load_data_from_gcs(self, source_uri: str) -> int:
        """
        Loads cleaned telemetry CSV data from GCS or mock storage to the BigQuery table.
        Returns the number of rows loaded.
        """
        logger.info(f"Starting data load job into BigQuery from source: {source_uri}")
        
        if self.mock_mode:
            # For mock mode, the source_uri is a file:// link. We can read it and insert it into DuckDB.
            local_path = source_uri.replace("file://", "")
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"[MOCK] Source data file not found: {local_path}")
                
            # Perform direct SQL copy using DuckDB's CSV reader
            load_sql = f"""
            INSERT INTO {self.dataset_id}.{self.table_id}
            SELECT * FROM read_csv_auto('{local_path}');
            """
            self.con.execute(load_sql)
            
            # Fetch total rows loaded in this batch
            row_count_res = self.con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{local_path}')").fetchone()
            row_count = row_count_res[0] if row_count_res else 0
            
            total_db_rows = self.con.execute(f"SELECT COUNT(*) FROM {self.dataset_id}.{self.table_id}").fetchone()[0]
            logger.info(f"[MOCK] Loaded {row_count} rows into DuckDB. Total table rows: {total_db_rows}")
            return row_count

        # Real GCP Mode execution
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(config.GCP_PROJECT_ID, self.dataset_id),
            self.table_id
        )
        
        # Load Job Configuration
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("aircraft_id", "STRING"),
                bigquery.SchemaField("aircraft_model", "STRING"),
                bigquery.SchemaField("engine_temperature", "FLOAT"),
                bigquery.SchemaField("altitude", "FLOAT"),
                bigquery.SchemaField("fuel_flow_rate", "FLOAT"),
                bigquery.SchemaField("vibration_level", "FLOAT"),
                bigquery.SchemaField("error_code", "STRING"),
            ],
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Header row is present
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False  # Schema explicitly defined above to guarantee constraints
        )
        
        try:
            load_job = self.client.load_table_from_uri(
                source_uri,
                table_ref,
                job_config=job_config
            )
            logger.info(f"Launched BigQuery Load Job: {load_job.job_id}. Waiting for completion...")
            load_job.result()  # Wait for the job to complete.
            
            # Verify result
            table = self.client.get_table(table_ref)
            logger.info(f"BigQuery Load complete. Loaded {load_job.output_rows} rows.")
            logger.info(f"Target table now contains {table.num_rows} rows.")
            return load_job.output_rows
        except Exception as e:
            logger.error(f"BigQuery Load Job failed for source {source_uri}: {str(e)}", exc_info=True)
            raise

    def query_table(self, query: str) -> pd.DataFrame:
        """
        Executes an analytical query against the BigQuery table.
        In mock mode, runs against DuckDB.
        """
        logger.info(f"Running analytical query:\n{query}")
        if self.mock_mode:
            # Query DuckDB
            return self.con.execute(query).df()
        else:
            # Query BigQuery
            query_job = self.client.query(query)
            return query_job.to_dataframe()
            
    def close(self):
        """Cleans up database connections."""
        if self.mock_mode and hasattr(self, "con"):
            self.con.close()
            logger.info("[MOCK] DuckDB connection closed.")
