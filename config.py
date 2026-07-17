import os
import logging

# Configure logging format and level globally
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)

# GCP Project and Infrastructure configurations
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "aircraft-reliability-lake-project")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "aircraft-telemetry-lake")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "aircraft_telemetry_lake")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "telemetry_daily")
LOCATION = os.getenv("GCP_LOCATION", "US")

# Ingestion configuration
# Standard paths for raw and processed zones
RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"

# Simulator settings
NUM_AIRCRAFT = 500
AIRCRAFT_MODELS = [
    "Boeing 737-800",
    "Airbus A320",
    "Boeing 787-9",
    "Airbus A350-900"
]
# Number of steps or logs generated per aircraft for a single batch run
SIMULATION_STEPS_PER_AIRCRAFT = 120  # e.g., 2 hours of flight logging at 1-minute intervals
CORRUPTION_RATE = 0.15  # Exact 15% rate of injected noise

# Sensor physical boundaries for validating physical anomalies (Phase 3)
SENSOR_BOUNDARIES = {
    "engine_temperature": {"min": 0.0, "max": 1200.0},     # Celsius
    "altitude": {"min": -100.0, "max": 45000.0},            # Feet (allow minor ground below sea-level)
    "fuel_flow_rate": {"min": 0.0, "max": 12000.0},         # kg/h
    "vibration_level": {"min": 0.0, "max": 10.0}            # Standard vibration index scale
}

# Run mode determination: fall back to local mock mode if GCP environment is missing
# This allows testing the entire pipeline locally without active GCP credentials.
has_gcp_creds = (
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS") is not None
    or os.getenv("GOOGLE_CLOUD_PROJECT") is not None
    # Check if run under gcloud context
    or os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json"))
)
MOCK_MODE = os.getenv("MOCK_MODE", str(not has_gcp_creds)).lower() in ("true", "1", "yes")

# Local directories used when running in MOCK_MODE
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "./data/gcs_mock")
LOCAL_DB_PATH = os.path.join(LOCAL_DATA_DIR, "bq_mock.db")

# Ensure local directories exist
if MOCK_MODE:
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
