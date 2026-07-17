import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import random
from typing import Tuple, Dict, List

import config

logger = logging.getLogger("DataSimulator")

class DataSimulator:
    """
    Generates synthetic flight telemetry data for a fleet of aircraft,
    and injects controlled corruption/missing data to simulate real-world IoT streams.
    """
    
    def __init__(self, num_aircraft: int = config.NUM_AIRCRAFT, 
                 models: List[str] = config.AIRCRAFT_MODELS, 
                 steps_per_aircraft: int = config.SIMULATION_STEPS_PER_AIRCRAFT,
                 corruption_rate: float = config.CORRUPTION_RATE):
        self.num_aircraft = num_aircraft
        self.models = models
        self.steps_per_aircraft = steps_per_aircraft
        self.corruption_rate = corruption_rate
        
        # Consistent mapping of aircraft_id to aircraft_model
        self.aircraft_fleet = self._initialize_fleet()
        
    def _initialize_fleet(self) -> Dict[str, str]:
        """Maps each aircraft ID to a specific aircraft model."""
        fleet = {}
        for idx in range(1, self.num_aircraft + 1):
            ac_id = f"AC{idx:03d}"
            # Keep model selection deterministic or pseudo-random based on ID
            random.seed(idx)
            model = random.choice(self.models)
            fleet[ac_id] = model
        logger.info(f"Initialized fleet of {self.num_aircraft} aircraft.")
        return fleet

    def generate_clean_telemetry(self, start_time: datetime) -> pd.DataFrame:
        """
        Generates healthy baseline telemetry for all aircraft in the fleet.
        Each aircraft has sequential records spaced 1 minute apart.
        """
        logger.info("Generating baseline clean telemetry data...")
        data_records = []
        
        for ac_id, model in self.aircraft_fleet.items():
            # Seed pseudo-random generator per aircraft to ensure reproducible normal variations
            ac_seed = int(ac_id.replace("AC", ""))
            np.random.seed(ac_seed)
            
            # Simulated base flight values
            base_temp = np.random.uniform(700.0, 750.0)
            base_alt = np.random.uniform(32000.0, 38000.0)
            base_fuel = np.random.uniform(3000.0, 4500.0)
            base_vib = np.random.uniform(1.2, 1.8)
            
            for step in range(self.steps_per_aircraft):
                timestamp = start_time + timedelta(minutes=step)
                
                # Introduce slight random walk for normal flight telemetry
                engine_temp = base_temp + np.random.normal(0, 5)
                altitude = base_alt + np.random.normal(0, 100)
                fuel_flow = base_fuel + np.random.normal(0, 20)
                vibration = max(0.1, base_vib + np.random.normal(0, 0.05))
                
                # Clamp normal flight values to physically reasonable boundaries
                engine_temp = np.clip(engine_temp, 650.0, 850.0)
                altitude = np.clip(altitude, 0.0, 42000.0)
                fuel_flow = np.clip(fuel_flow, 1000.0, 6000.0)
                vibration = np.clip(vibration, 0.5, 4.0)
                
                data_records.append({
                    "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "aircraft_id": ac_id,
                    "aircraft_model": model,
                    "engine_temperature": float(engine_temp),
                    "altitude": float(altitude),
                    "fuel_flow_rate": float(fuel_flow),
                    "vibration_level": float(vibration),
                    "error_code": None
                })
                
        df = pd.DataFrame(data_records)
        logger.info(f"Generated {len(df)} base clean records.")
        return df

    def inject_corruption(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Intentionally injects exactly self.corruption_rate (15%) missing or corrupted records
        into the generated telemetry dataset.
        """
        total_records = len(df)
        num_corrupt = int(total_records * self.corruption_rate)
        logger.info(f"Injecting corruption into exactly {num_corrupt} records ({self.corruption_rate * 100}% of {total_records})...")
        
        # Randomly choose indices to corrupt (without replacement)
        np.random.seed(42)  # For consistent simulation output
        corrupt_indices = np.random.choice(df.index, size=num_corrupt, replace=False)
        
        # Set of potential error codes for faults
        telemetry_errors = ["ERR_ENG_TEMP_HIGH", "ERR_ALT_SND_FAIL", "ERR_FUEL_LEAK", "ERR_SYS_VIB_HIGH"]
        
        # Distribute the corruptions across four categories:
        # 1. Missing structural columns (timestamp or aircraft_id is null)
        # 2. Missing sensor columns (one or more sensor values are NaN)
        # 3. Unphysical extreme outliers (sensors showing impossible physical readings)
        # 4. Standard failures (elevated sensor values paired with error codes)
        
        for idx in corrupt_indices:
            corruption_type = np.random.choice(["structural_null", "sensor_null", "outlier", "fault_code"])
            
            if corruption_type == "structural_null":
                # Missing essential keys (50% timestamp null, 50% aircraft_id null)
                if np.random.rand() < 0.5:
                    df.at[idx, "timestamp"] = None
                else:
                    df.at[idx, "aircraft_id"] = None
                    
            elif corruption_type == "sensor_null":
                # Sensor value set to NaN (randomly pick one or more sensors)
                sensors = ["engine_temperature", "altitude", "fuel_flow_rate", "vibration_level"]
                sensor_to_null = np.random.choice(sensors)
                df.at[idx, sensor_to_null] = np.nan
                
            elif corruption_type == "outlier":
                # Unphysical extreme values
                sensor = np.random.choice(["engine_temperature", "altitude", "fuel_flow_rate", "vibration_level"])
                if sensor == "engine_temperature":
                    # Unrealistic extreme (either freezing or melting engine)
                    df.at[idx, "engine_temperature"] = np.random.choice([-999.0, 5500.0])
                elif sensor == "altitude":
                    # Flight level higher than satellite or negative
                    df.at[idx, "altitude"] = np.random.choice([-15000.0, 120000.0])
                elif sensor == "fuel_flow_rate":
                    # Negative fuel flow or massive flow rate
                    df.at[idx, "fuel_flow_rate"] = np.random.choice([-500.0, 95000.0])
                elif sensor == "vibration_level":
                    # Extreme vibration level or negative
                    df.at[idx, "vibration_level"] = np.random.choice([-5.0, 99.0])
                    
            elif corruption_type == "fault_code":
                # Real fault representation: elevated parameters paired with error_code
                error = np.random.choice(telemetry_errors)
                df.at[idx, "error_code"] = error
                if error == "ERR_ENG_TEMP_HIGH":
                    df.at[idx, "engine_temperature"] = float(np.random.uniform(980.0, 1100.0))
                elif error == "ERR_SYS_VIB_HIGH":
                    df.at[idx, "vibration_level"] = float(np.random.uniform(6.5, 9.0))
                elif error == "ERR_ALT_SND_FAIL":
                    df.at[idx, "altitude"] = float(np.random.uniform(0.0, 500.0))
                elif error == "ERR_FUEL_LEAK":
                    df.at[idx, "fuel_flow_rate"] = float(np.random.uniform(8000.0, 10000.0))
                    
        # Verify corruption rate in the final dataset
        logger.info("Corruption injection complete.")
        return df

    def run_simulation(self, start_time: datetime = None) -> pd.DataFrame:
        """
        Runs the full simulation pipeline: generates base clean telemetry,
        injects exactly 15% corruption, and returns the final DataFrame.
        """
        if start_time is None:
            # Default to current UTC time truncated to hour
            start_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            
        logger.info(f"Starting telemetry simulation at virtual start time: {start_time}")
        try:
            clean_df = self.generate_clean_telemetry(start_time)
            corrupted_df = self.inject_corruption(clean_df)
            
            # Print statistics of the output
            logger.info("--- Simulated Data Statistics ---")
            logger.info(f"Total simulated rows: {len(corrupted_df)}")
            null_timestamps = corrupted_df["timestamp"].isna().sum()
            null_aircraft = corrupted_df["aircraft_id"].isna().sum()
            null_sensors = corrupted_df[["engine_temperature", "altitude", "fuel_flow_rate", "vibration_level"]].isna().any(axis=1).sum()
            logger.info(f"Records with null timestamp: {null_timestamps}")
            logger.info(f"Records with null aircraft_id: {null_aircraft}")
            logger.info(f"Records with null sensor values: {null_sensors}")
            logger.info(f"Records with actual error codes: {corrupted_df['error_code'].notna().sum()}")
            
            return corrupted_df
        except Exception as e:
            logger.error(f"Error during simulation execution: {str(e)}", exc_info=True)
            raise
