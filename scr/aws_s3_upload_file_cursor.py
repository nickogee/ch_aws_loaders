import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import uuid
import gzip
import pandas as pd
from botocore.exceptions import ClientError
from boto3.session import Session

import tempfile
import shutil
from config.cred.enviroment import Environment


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
            self.compress_to_gzip()
            return self.upload_file()
        except S3UploaderError as e:
            self.logger.error(f"Upload process failed: {str(e)}")
            raise


class TempDir:
    """Context manager for a temporary directory lifecycle."""
    def __init__(self):
        self.dir: Optional[Path] = None

    def __enter__(self):
        self.dir = Path(tempfile.mkdtemp(prefix='uploader_'))
        return self.dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.dir and self.dir.exists():
            shutil.rmtree(self.dir)
