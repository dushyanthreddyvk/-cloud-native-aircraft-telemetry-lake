import os
import logging
import io
import pandas as pd
from typing import Optional

# Conditional import for GCS to avoid failure if dependencies aren't set up
# but since they're in requirements.txt, we import standardly.
try:
    from google.cloud import storage
    from google.api_core.exceptions import Conflict
except ImportError:
    storage = None
    Conflict = Exception

import config

logger = logging.getLogger("GCSStorageManager")

class GCSStorageManager:
    """
    Manages interactions with Google Cloud Storage (GCS).
    Supports programmatic bucket creation and dataframe upload/download.
    Includes a Local Mock Mode for testing without active GCP credentials.
    """
    
    def __init__(self, mock_mode: bool = config.MOCK_MODE):
        self.mock_mode = mock_mode
        self.client = None
        
        if self.mock_mode:
            logger.info("GCSStorageManager running in LOCAL MOCK MODE.")
            self.mock_dir = config.LOCAL_DATA_DIR
            os.makedirs(self.mock_dir, exist_ok=True)
        else:
            logger.info("GCSStorageManager running in GCP CLOUD MODE.")
            if storage is None:
                raise ImportError("google-cloud-storage library is missing or cannot be imported.")
            self.client = storage.Client(project=config.GCP_PROJECT_ID)

    def create_bucket_if_not_exists(self, bucket_name: str, location: str = config.LOCATION) -> bool:
        """
        Creates a GCS bucket if it does not already exist.
        In mock mode, creates a local directory.
        """
        if self.mock_mode:
            bucket_path = os.path.join(self.mock_dir, bucket_name)
            os.makedirs(bucket_path, exist_ok=True)
            logger.info(f"[MOCK] Simulated GCS Bucket directory created at: {bucket_path}")
            return True
            
        try:
            bucket = self.client.bucket(bucket_name)
            if not bucket.exists():
                bucket.storage_class = "STANDARD"
                self.client.create_bucket(bucket, location=location)
                logger.info(f"Successfully created GCS Bucket: gs://{bucket_name} in location {location}")
                return True
            else:
                logger.info(f"GCS Bucket gs://{bucket_name} already exists.")
                return False
        except Conflict:
            logger.info(f"GCS Bucket gs://{bucket_name} already exists (concurrency conflict).")
            return False
        except Exception as e:
            logger.error(f"Failed to create/verify GCS bucket gs://{bucket_name}: {str(e)}", exc_info=True)
            raise

    def upload_dataframe(self, df: pd.DataFrame, bucket_name: str, object_key: str, file_format: str = "csv") -> str:
        """
        Uploads a Pandas DataFrame directly to GCS (or mock folder) in the specified format.
        Returns the absolute URI/path of the uploaded file.
        """
        # Ensure clean directory structures in object key (remove double slashes or leading slashes)
        object_key = object_key.lstrip("/")
        
        if self.mock_mode:
            local_dest = os.path.join(self.mock_dir, bucket_name, object_key)
            os.makedirs(os.path.dirname(local_dest), exist_ok=True)
            
            if file_format.lower() == "csv":
                df.to_csv(local_dest, index=False)
            elif file_format.lower() == "parquet":
                df.to_parquet(local_dest, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
            local_uri = f"file://{local_dest}"
            logger.info(f"[MOCK] Uploaded DataFrame to mock storage: {local_uri} (Shape: {df.shape})")
            return local_uri
            
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_key)
            
            # Use buffer to write dataframe in-memory instead of writing to local disk first
            buffer = io.BytesIO()
            if file_format.lower() == "csv":
                df.to_csv(buffer, index=False, encoding="utf-8")
                content_type = "text/csv"
            elif file_format.lower() == "parquet":
                df.to_parquet(buffer, index=False)
                content_type = "application/octet-stream"
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
            buffer.seek(0)
            blob.upload_from_file(buffer, content_type=content_type)
            
            gcs_uri = f"gs://{bucket_name}/{object_key}"
            logger.info(f"Successfully uploaded DataFrame to GCS: {gcs_uri} (Shape: {df.shape})")
            return gcs_uri
        except Exception as e:
            logger.error(f"Failed to upload DataFrame to gs://{bucket_name}/{object_key}: {str(e)}", exc_info=True)
            raise

    def download_dataframe(self, bucket_name: str, object_key: str, file_format: str = "csv") -> pd.DataFrame:
        """
        Downloads an object from GCS (or mock folder) and loads it into a Pandas DataFrame.
        """
        object_key = object_key.lstrip("/")
        
        if self.mock_mode:
            local_src = os.path.join(self.mock_dir, bucket_name, object_key)
            if not os.path.exists(local_src):
                raise FileNotFoundError(f"[MOCK] Mock storage object not found: {local_src}")
                
            if file_format.lower() == "csv":
                df = pd.read_csv(local_src)
            elif file_format.lower() == "parquet":
                df = pd.read_parquet(local_src)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
            logger.info(f"[MOCK] Downloaded DataFrame from mock storage: {local_src} (Shape: {df.shape})")
            return df
            
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_key)
            
            if not blob.exists():
                raise FileNotFoundError(f"GCS object not found: gs://{bucket_name}/{object_key}")
                
            data = blob.download_as_bytes()
            buffer = io.BytesIO(data)
            
            if file_format.lower() == "csv":
                df = pd.read_csv(buffer)
            elif file_format.lower() == "parquet":
                df = pd.read_parquet(buffer)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
            logger.info(f"Successfully downloaded DataFrame from GCS: gs://{bucket_name}/{object_key} (Shape: {df.shape})")
            return df
        except Exception as e:
            logger.error(f"Failed to download DataFrame from gs://{bucket_name}/{object_key}: {str(e)}", exc_info=True)
            raise
