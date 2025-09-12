import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import uuid
import gzip
import pandas as pd
from botocore.exceptions import ClientError
from boto3.session import Session

from config.cred.enviroment import Environment
import os
import json
import tempfile
from google.cloud import bigquery
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.json as paj

@dataclass
class S3Config:
    """Configuration for S3 connection and upload."""
    access_key: str
    secret_key: str
    bucket_name: str
    region_name: str


class S3UploaderError(Exception):
    """Base exception for S3Uploader errors."""
    pass


class FileNotFoundError(S3UploaderError):
    """Raised when a required file is not found."""
    pass


class S3UploadError(S3UploaderError):
    """Raised when S3 upload fails."""
    pass


class S3Uploader():
    """Handles the process of converting, compressing, and uploading files to S3."""

    def __init__(self, entity_path: str, dt_partition: datetime, dt_now: Optional[datetime] = None):
        """
        Initialize the S3Uploader.

        Args:
            entity_path: The entity path for file
            dt_now: datetime for the S3 path part. Defaults to current time in UTC.
            dt_partition: datetime of the bq table partition. In UTC.
        """
        self._setup_logging()
        self._load_config()
        self.entity_path = entity_path
        self.dt_now = dt_now or datetime.now(timezone.utc)
        self.dt_partition = dt_partition
        self.hash_string = self._generate_hash(8)
        self._setup_paths()
        self._setup_s3_client()

    def _setup_logging(self) -> None:
        """Configure logging for the uploader."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _load_config(self) -> None:
        """Load S3 configuration from environment."""
        env = Environment()
        self.config = S3Config(
            access_key=env.aws_s3_access_key,
            secret_key=env.aws_s3_secret_key,
            bucket_name=env.aws_s3_bucket_name,
            region_name=env.aws_s3_region_name
        )

    def _setup_paths(self) -> None:
        """Initialize file paths."""
        self.json_path: Optional[Path] = None
        self.parquet_path: Optional[Path] = None
        self.gzip_path: Optional[Path] = None
        self.s3_parent_path_file_key = f'{self.entity_path}/{self.dt_partition:%Y-%m-%d}/'
        self.s3_full_file_key = (
            self.s3_parent_path_file_key + f'{self.hash_string}_{self.dt_now:%H:%M:%S}.parquet.gz'
        )

    def _setup_s3_client(self) -> None:
        """Set up S3 client."""
        session = Session(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            region_name=self.config.region_name
        )
        self.s3 = session.resource('s3')
        self.bucket = self.s3.Bucket(self.config.bucket_name)


    @staticmethod
    def _generate_hash(n: int) -> str:
        """Generate a random hash string of specified length."""
        return str(uuid.uuid4())[:n]

    def set_json_path(self, path: str) -> None:
        """
        Set the JSON file path and derive other paths.

        Args:
            path: Path to the JSON file
        """
        self.json_path = Path(path)
        if not self.json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {path}")
        
        # Derive other paths
        base_path = str(self.json_path)[:str(self.json_path).rfind(".json")]
        self.parquet_path = Path(f"{base_path}.parquet")
        self.gzip_path = Path(f"{base_path}.parquet.gz")

    def convert_to_parquet(self) -> None:
        """Convert JSON file to Parquet format."""
        if not self.json_path:
            raise FileNotFoundError("JSON path not set")

        self.logger.info(f"Converting {self.json_path} to Parquet format")
        try:
            data = pd.read_json(self.json_path)
            self.logger.info(f'---- Write to Json - {len(data)} rows')

            # Sanitize all columns to avoid Arrow type inference issues
            def sanitize_value(v):
                # Handle numpy arrays explicitly to avoid ambiguous truth value
                if isinstance(v, np.ndarray):
                    if v.size == 0:
                        return None
                    v = v.tolist()
                    try:
                        return json.dumps(v, ensure_ascii=False, default=str)
                    except Exception:
                        return str(v)

                # Handle dicts and lists
                if isinstance(v, (dict, list)):
                    if isinstance(v, dict) and not v:
                        return None
                    if isinstance(v, list) and len(v) == 0:
                        return None
                    try:
                        return json.dumps(v, ensure_ascii=False, default=str)
                    except Exception:
                        return str(v)

                # Only call pd.isna for scalar values
                try:
                    import pandas.api.types as ptypes
                    if ptypes.is_scalar(v) and pd.isna(v):
                        return None
                except Exception:
                    pass

                return v

            # Apply sanitization and cast object columns to pandas nullable string
            for col in data.columns:
                data[col] = data[col].apply(sanitize_value)
                if data[col].dtype == object:
                    data[col] = data[col].astype('string')

            data.to_parquet(self.parquet_path)
            # Check number of rows in the saved Parquet file
            try:
                df_check = pd.read_parquet(self.parquet_path)
                self.logger.info(f'---- Number of rows in the saved Parquet file: {len(df_check)}')
            except Exception as e:
                self.logger.error(f'Error reading Parquet file for row count: {e}')
            self.logger.info(f"Successfully converted to {self.parquet_path}")
        except Exception as e:
            self.logger.error(f"Failed to convert to Parquet: {str(e)}")
            raise S3UploaderError(f"Parquet conversion failed: {str(e)}")

    def convert_to_parquet_pyarrow(self) -> None:
        """
        Convert JSON to Parquet using PyArrow instead of pandas.

        Supports both NDJSON (one JSON object per line) and JSON array files.
        Performs light preprocessing to replace empty dict/list with None to avoid
        empty struct/list issues in Parquet.
        """
        if not self.json_path:
            raise FileNotFoundError("JSON path not set")

        self.logger.info(f"[PyArrow] Converting {self.json_path} to Parquet format")
        try:
            # Detect NDJSON vs JSON array by peeking first non-whitespace character
            with open(self.json_path, 'r', encoding='utf-8') as f:
                content_start = None
                while True:
                    ch = f.read(1)
                    if not ch:
                        break
                    if not ch.isspace():
                        content_start = ch
                        break

            def replace_empty(obj):
                if isinstance(obj, dict):
                    if not obj:
                        return None
                    return {k: replace_empty(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    if len(obj) == 0:
                        return None
                    return [replace_empty(v) for v in obj]
                if isinstance(obj, np.ndarray):
                    if obj.size == 0:
                        return None
                    return obj.tolist()
                return obj

            if content_start == '{':
                # Likely NDJSON (object per line) or a single object
                # Try using pyarrow.json which expects NDJSON
                try:
                    table = paj.read_json(str(self.json_path))
                except Exception:
                    # Fallback: read lines manually and build pylist
                    records = []
                    with open(self.json_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                obj = json.loads(line)
                            except Exception:
                                continue
                            records.append(replace_empty(obj))
                    table = pa.Table.from_pylist(records)
            else:
                # Assume JSON array
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    data = [data]
                records = [replace_empty(rec) for rec in data]
                table = pa.Table.from_pylist(records)

            # Write Parquet with compression
            pq.write_table(table, str(self.parquet_path), compression='snappy')

            # Verify number of rows
            try:
                pf = pq.ParquetFile(str(self.parquet_path))
                num_rows = pf.metadata.num_rows if pf.metadata is not None else None
                self.logger.info(f"[PyArrow] Number of rows in saved Parquet file: {num_rows}")
            except Exception as e:
                self.logger.error(f"[PyArrow] Error reading Parquet file for row count: {e}")

            self.logger.info(f"[PyArrow] Successfully converted to {self.parquet_path}")
        except Exception as e:
            self.logger.error(f"[PyArrow] Failed to convert to Parquet: {str(e)}")
            raise S3UploaderError(f"Parquet conversion failed: {str(e)}")

    def compress_to_gzip(self) -> None:
        """Compress Parquet file to gzip format."""
        if not self.parquet_path or not self.parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.parquet_path}")

        self.logger.info(f"Compressing {self.parquet_path} to gzip format")
        try:
            with open(self.parquet_path, 'rb') as src, gzip.open(self.gzip_path, 'wb') as dst:
                dst.writelines(src)
            self.logger.info(f"Successfully compressed to {self.gzip_path}")
        except Exception as e:
            self.logger.error(f"Failed to compress to gzip: {str(e)}")
            raise S3UploaderError(f"Gzip compression failed: {str(e)}")

    def _clear_s3_path(self, s3_path: str) -> None:
        """
        Delete all files from the specified S3 path.
        
        Args:
            s3_path: The S3 path to clear (e.g., 'partner_metrics/entity/2025-01-15/')
            
        Raises:
            S3UploaderError: If clearing the path fails.
        """
        if not s3_path.endswith('/'):
            s3_path += '/'
            
        self.logger.info(f"Clearing S3 path: s3://{self.config.bucket_name}/{s3_path}")
        
        try:
            # List all objects in the path
            objects_to_delete = []
            for obj in self.bucket.objects.filter(Prefix=s3_path):
                objects_to_delete.append({'Key': obj.key})
                self.logger.debug(f"Found object to delete: {obj.key}")
            
            if not objects_to_delete:
                self.logger.info(f"No files found in path: {s3_path}")
                return
            
            # Delete all objects in batches (S3 allows up to 1000 objects per delete request)
            batch_size = 1000
            for i in range(0, len(objects_to_delete), batch_size):
                batch = objects_to_delete[i:i + batch_size]
                response = self.bucket.delete_objects(Delete={'Objects': batch})
                
                # Log any errors from the delete operation
                if 'Errors' in response:
                    for error in response['Errors']:
                        self.logger.error(f"Failed to delete {error['Key']}: {error['Message']}")
                
                self.logger.info(f"Deleted batch of {len(batch)} objects from {s3_path}")
            
            self.logger.info(f"Successfully cleared S3 path: {s3_path}")
            
        except ClientError as e:
            self.logger.error(f"Failed to clear S3 path {s3_path}: {e}")
            raise S3UploaderError(f"Failed to clear S3 path {s3_path}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error while clearing S3 path {s3_path}: {e}")
            raise S3UploaderError(f"Unexpected error while clearing S3 path {s3_path}: {e}")


    def verify_s3_upload(self, key: str) -> bool:
        """
        Verify that a file exists in S3 bucket.

        Args:
            key: The S3 object key to verify

        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3.Object(self.config.bucket_name, key).load()
            self.logger.info(f"Verified file exists in S3: {key}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.error(f"File not found in S3: {key}")
                return False
            else:
                self.logger.error(f"Error verifying S3 file: {str(e)}")
                raise S3UploadError(f"Failed to verify S3 file: {str(e)}")


    def upload_file(self) -> bool:
        """
        Upload the gzipped file to S3.

        Returns:
            bool: True if upload was successful, False otherwise
        """
        if not self.gzip_path or not self.gzip_path.exists():
            raise FileNotFoundError(f"Gzip file not found: {self.gzip_path}")

        self.logger.info(f"Uploading {self.gzip_path} to S3")
        try:
            # Clear the S3 path before uploading
            s3_path_to_clear = self.s3_parent_path_file_key
            self.logger.info(f"Clearing S3 path before upload: {s3_path_to_clear}")
            self._clear_s3_path(s3_path_to_clear)

            self.bucket.upload_file(
                Key=self.s3_full_file_key,
                Filename=str(self.gzip_path)
            )

            # Verify the upload
            if self.verify_s3_upload(self.s3_full_file_key):
                self.logger.info(f"Successfully uploaded and verified {self.s3_full_file_key}")
                return True
            else:
                self.logger.error("Upload verification failed")
                return False
            
        except ClientError as e:
            self.logger.error(f"S3 upload failed: {str(e)}")
            raise S3UploadError(f"Failed to upload to S3: {str(e)}")

    def run(self) -> bool:
        """
        Execute the complete upload process.

        Returns:
            bool: True if all steps completed successfully

        Raises:
            S3UploaderError: If any step in the process fails
        """
        try:
            self.convert_to_parquet()
            # self.convert_to_parquet_pyarrow()
            self.compress_to_gzip()
            return self.upload_file()
        except S3UploaderError as e:
            self.logger.error(f"Upload process failed: {str(e)}")
            raise


class NaNEncoder(json.JSONEncoder):
    def default(self, obj):
        if pd.isna(obj) or (isinstance(obj, float) and np.isnan(obj)):
            return ""
        return super().default(obj)

def clean_nan_values(obj):
    """Recursively clean NaN values from dictionaries and lists"""
    import numpy as np
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, np.ndarray):
        # Convert numpy arrays to lists and clean recursively
        return clean_nan_values(obj.tolist())
    elif pd.isna(obj) or (isinstance(obj, float) and np.isnan(obj)):
        return ""
    else:
        return obj

class S3LogsToBigQueryLoader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.env = Environment()
        self.bucket_name = self.env.aws_s3_bucket_name
        self.access_key = self.env.aws_s3_access_key
        self.secret_key = self.env.aws_s3_secret_key
        self.region_name = self.env.aws_s3_region_name
        self.bq_client = self.env.bq_client
        self._setup_s3()

    def _setup_s3(self):
        session = Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name
        )
        self.s3 = session.resource('s3')
        self.bucket = self.s3.Bucket(self.bucket_name)

    def load_to_bigquery(self, s3_prefix: str, bq_table: str, partition_dt: datetime):
        # Get UTC timestamp for created_at field
        # partition_dt = datetime.strptime(str(partition_str), '%Y-%m-%d')
        
        # Add partition decorator to table name to overwrite only that partition
        partition_date = partition_dt.strftime('%Y%m%d')
        partitioned_table = f"{bq_table}${partition_date}"
        
        self.logger.info(f"Listing files in s3://{self.bucket_name}/{s3_prefix} with suffix .log.gz or .parquet.gz")
        s3_files = [
            obj.key for obj in self.bucket.objects.filter(Prefix=s3_prefix)
            if obj.key.endswith('.log.gz') or obj.key.endswith('.parquet.gz')
        ]
        if not s3_files:
            self.logger.warning(f"No .log.gz or .parquet.gz files found in s3://{self.bucket_name}/{s3_prefix}")
            return
        self.logger.info(f"Found {len(s3_files)} files to process.")

        all_records = []
        with tempfile.TemporaryDirectory() as tmpdir:
            for s3_key in s3_files:
                local_path = Path(tmpdir) / Path(s3_key).name
                self.logger.info(f"Downloading {s3_key} to {local_path}")
                try:
                    self.bucket.download_file(s3_key, str(local_path))
                except ClientError as e:
                    self.logger.error(f"Failed to download {s3_key}: {e}")
                    continue
                try:
                    if s3_key.endswith('.log.gz'):
                        with gzip.open(local_path, 'rt', encoding='utf-8') as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                all_records.append({
                                    "id": str(uuid.uuid4()),
                                    "raw_data": clean_nan_values(json.loads(line)),
                                    "created_at": partition_dt.isoformat()
                                })
                    elif s3_key.endswith('.parquet.gz'):
                        
                        parquet_path = Path(tmpdir) / (Path(s3_key).stem)
                        with gzip.open(local_path, 'rb') as gz_in, open(parquet_path, 'wb') as pq_out:
                            pq_out.write(gz_in.read())
                        df = pd.read_parquet(parquet_path)
                        for record in df.to_dict(orient='records'):
                            all_records.append({
                                "id": str(uuid.uuid4()),
                                "raw_data": clean_nan_values(record),
                                "created_at": partition_dt.isoformat()
                            })
                except Exception as e:
                    self.logger.error(f"Failed to process {local_path}: {e}")

        if not all_records:
            self.logger.warning("No records to load into BigQuery.")
            return

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            ),
        )

        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as ndjson_file:
            for record in all_records:
                ndjson_file.write(json.dumps(record, cls=NaNEncoder, default=str) + '\n')
            ndjson_path = ndjson_file.name
        try:
            with open(ndjson_path, 'rb') as source_file:
                load_job = self.bq_client.load_table_from_file(
                    source_file, partitioned_table, job_config=job_config
                )
            load_job.result()
            if load_job.errors:
                self.logger.error(f"BigQuery load job failed: {load_job.errors}")
            else:
                self.logger.info(f"Successfully loaded data to {partitioned_table}. Job ID: {load_job.job_id}")
        finally:
            os.remove(ndjson_path)

