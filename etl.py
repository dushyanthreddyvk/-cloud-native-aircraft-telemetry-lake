import logging
import pandas as pd
import numpy as np
from typing import List

import config

logger = logging.getLogger("TelemetryETLEngine")

class TelemetryETLEngine:
    """
    Cleans raw aircraft telemetry data by standardizing schemas, dropping structural nulls,
    detecting and correcting physical outliers, and imputing missing sensor values.
    """
    
    def __init__(self):
        self.sensor_cols = ["engine_temperature", "altitude", "fuel_flow_rate", "vibration_level"]
        self.boundaries = config.SENSOR_BOUNDARIES

    def clean_telemetry(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the full ETL data cleaning pipeline on the input DataFrame.
        """
        logger.info("Starting ETL data cleaning pipeline...")
        df = raw_df.copy()
        initial_row_count = len(df)
        
        # 1. Handle Critical Structural Nulls
        # Drop records that do not have critical identifiers (timestamp or aircraft_id)
        df = df.dropna(subset=["timestamp", "aircraft_id"])
        dropped_structural = initial_row_count - len(df)
        logger.info(f"Dropped {dropped_structural} records due to missing structural columns (timestamp/aircraft_id).")
        
        # Ensure timestamp is cast to Datetime and standardized to ISO 8601 UTC
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            # Drop any rows where timestamp conversion failed and resulted in NaT
            nan_timestamps = df["timestamp"].isna().sum()
            if nan_timestamps > 0:
                df = df.dropna(subset=["timestamp"])
                logger.info(f"Dropped {nan_timestamps} records due to unparseable timestamps.")
        except Exception as e:
            logger.error(f"Error parsing timestamp column: {str(e)}")
            raise

        # 2. Convert Data Types
        # Force strict casting of columns to match target BigQuery schema
        df["aircraft_id"] = df["aircraft_id"].astype(str)
        df["aircraft_model"] = df["aircraft_model"].astype(str)
        df["error_code"] = df["error_code"].fillna("").astype(str).replace("", None)
        
        for col in self.sensor_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

        # 3. Detect and Replace Physical Sensor Outliers with NaN
        # Instead of throwing away records with bad sensor readings, we blank out the bad readings
        # and allow them to be imputed alongside other missing values.
        outliers_detected = 0
        for sensor, limits in self.boundaries.items():
            min_val = limits["min"]
            max_val = limits["max"]
            
            # Find elements violating physical boundaries
            outlier_mask = (df[sensor] < min_val) | (df[sensor] > max_val)
            cnt = outlier_mask.sum()
            if cnt > 0:
                outliers_detected += cnt
                logger.warning(f"Detected {cnt} unphysical outliers in '{sensor}' (outside range [{min_val}, {max_val}]). Setting to NaN for imputation.")
                df.loc[outlier_mask, sensor] = np.nan

        # 4. Impute Missing & Outlier Sensor Data
        # Group by aircraft_id and apply rolling average.
        # If rolling average fails (e.g. at boundary edges), use forward fill and backward fill per aircraft.
        logger.info("Imputing missing sensor data using rolling averages per aircraft...")
        
        # Sort values chronologically per aircraft to guarantee rolling window operates on time sequence
        df = df.sort_values(by=["aircraft_id", "timestamp"]).reset_index(drop=True)
        
        # Apply imputation per aircraft group
        df[self.sensor_cols] = df.groupby("aircraft_id")[self.sensor_cols].transform(
            lambda group: self._impute_group(group)
        )
        
        # If there are any remaining NaNs (e.g., if an entire aircraft dataset was null for a sensor),
        # fill with the overall global average of that sensor column.
        for col in self.sensor_cols:
            remaining_nans = df[col].isna().sum()
            if remaining_nans > 0:
                global_mean = df[col].mean()
                if pd.isna(global_mean):
                    # Fallback default values based on bounds mid-point if mean is completely NaN
                    global_mean = (self.boundaries[col]["min"] + self.boundaries[col]["max"]) / 2.0
                df[col] = df[col].fillna(global_mean)
                logger.info(f"Filled {remaining_nans} remaining NaNs in '{col}' with global mean value: {global_mean:.2f}")

        # Final schema sanity check
        assert df["timestamp"].isna().sum() == 0, "Structural constraint violated: NaN timestamps exist after ETL."
        assert df["aircraft_id"].isna().sum() == 0, "Structural constraint violated: NaN aircraft_ids exist after ETL."
        assert df[self.sensor_cols].isna().sum().sum() == 0, "Sensor constraint violated: NaN sensor values exist after ETL."
        
        # Re-format timestamp column to ISO 8601 string for file serialization (e.g., YYYY-MM-DDTHH:MM:SSZ)
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"ETL process completed. Initial: {initial_row_count} rows, Processed: {len(df)} rows. Outliers replaced: {outliers_detected}.")
        return df

    def _impute_group(self, group: pd.DataFrame) -> pd.DataFrame:
        """
        Helper method to impute missing sensor data for a single aircraft group.
        Uses a rolling average of window size 5, followed by forward fill and backward fill.
        """
        # Calculate rolling average per sensor. min_periods=1 ensures we get a value as long as 1 valid record is in the window.
        rolling_mean = group.rolling(window=5, min_periods=1, center=True).mean()
        
        # Fill missing values with the rolling mean
        imputed_group = group.fillna(rolling_mean)
        
        # Fallback forward fill and backward fill within the group for remaining NaNs
        imputed_group = imputed_group.ffill().bfill()
        
        return imputed_group
