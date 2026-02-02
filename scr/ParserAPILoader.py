import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import uuid
import json
import tempfile
import os
import requests
from google.cloud import bigquery
import pandas as pd
import numpy as np

from config.cred.enviroment import Environment


@dataclass
class APIConfig:
    """Configuration for API connection."""
    token: str
    headers: Dict[str, str]
    base_url: str
    route: str


class APILoaderError(Exception):
    """Base exception for APIToBigQueryLoader errors."""
    pass


class APIRequestError(APILoaderError):
    """Raised when API request fails."""
    pass


class BigQueryLoadError(APILoaderError):
    """Raised when BigQuery load fails."""
    pass


class NaNEncoder(json.JSONEncoder):
    """JSON encoder that handles NaN values."""
    def default(self, obj):
        if pd.isna(obj) or (isinstance(obj, float) and np.isnan(obj)):
            return ""
        return super().default(obj)


def clean_nan_values(obj):
    """Recursively clean NaN values from dictionaries and lists."""
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


class APIToBigQueryLoader:
    """Handles the process of fetching data from API and loading it to BigQuery."""

    def __init__(
        self,
        route: str,
        bq_table: str,
        partition_dt: Optional[datetime] = None,
    ):
        """
        Initialize the APIToBigQueryLoader.

        Args:
            token: API authentication token
            headers: Optional custom headers dictionary. If not provided, defaults to Authorization header with token.
            base_url: Base URL for the API
            route: API route/endpoint path
            bq_table: BigQuery table name (format: project.dataset.table)
            partition_dt: datetime for the BigQuery partition. Defaults to current time in UTC.
        """
        self.env = Environment()
        self.token = self.env.parser_token
        self.headers = self.env.parser_headers
        self.base_url = self.env.parser_base_url

        self._setup_logging()
        self._load_config()
        
        # Set up API configuration
        default_headers = self.headers or {}
        if 'Authorization' not in default_headers and self.token:
            default_headers['Authorization'] = f'Bearer {self.token}'
        if 'Content-Type' not in default_headers:
            default_headers['Content-Type'] = 'application/json'
        
        self.api_config = APIConfig(
            token=self.token,
            headers=default_headers,
            base_url=self.base_url,
            route=route
        )
        
        self.bq_table = bq_table
        self.partition_dt = partition_dt or datetime.now(timezone.utc)
        self._setup_bigquery_client()

    def _setup_logging(self) -> None:
        """Configure logging for the loader."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _load_config(self) -> None:
        """Load BigQuery configuration from environment."""
        env = Environment()
        self.bq_client = env.bq_client

    def _setup_bigquery_client(self) -> None:
        """Set up BigQuery client."""
        if not self.bq_client:
            raise APILoaderError("BigQuery client not available from environment configuration")

    def _build_api_url(self) -> str:
        """Build the full API URL from base_url and route."""
        base = self.api_config.base_url.rstrip('/')
        route = self.api_config.route.lstrip('/')
        return f"{base}/{route}"

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch JSON data from the API.

        Returns:
            List of records from the 'data' property of the API response

        Raises:
            APIRequestError: If the API request fails
        """
        url = self._build_api_url()
        self.logger.info(f"Fetching data from API: {url}")

        try:
            response = requests.get(
                url,
                headers=self.api_config.headers,
                # timeout=300  # 5 minutes timeout
            )
            response.raise_for_status()

            json_data = response.json()
            
            # Check if response has 'success' field
            if isinstance(json_data, dict) and 'success' in json_data:
                if not json_data.get('success', False):
                    error_msg = json_data.get('error', 'API returned success=false')
                    raise APIRequestError(f"API request failed: {error_msg}")
            
            # Extract data from 'data' property
            if isinstance(json_data, dict) and 'data' in json_data:
                data_records = json_data['data']
                if not isinstance(data_records, list):
                    raise APIRequestError(f"Expected 'data' to be a list, got {type(data_records)}")
                
                self.logger.info(f"Successfully fetched {len(data_records)} records from API")
                return data_records
            else:
                # If response is directly a list, use it
                if isinstance(json_data, list):
                    self.logger.info(f"Successfully fetched {len(json_data)} records from API")
                    return json_data
                else:
                    raise APIRequestError("API response does not contain 'data' property or is not a list")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise APIRequestError(f"Failed to fetch data from API: {str(e)}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {str(e)}")
            raise APIRequestError(f"Invalid JSON response from API: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error while fetching data: {str(e)}")
            raise APIRequestError(f"Unexpected error while fetching data: {str(e)}")

    def load_to_bigquery(self, records: List[Dict[str, Any]]) -> bool:
        """
        Load records to BigQuery table.

        Args:
            records: List of records to load

        Returns:
            bool: True if load was successful, False otherwise

        Raises:
            BigQueryLoadError: If the BigQuery load fails
        """
        if not records:
            self.logger.warning("No records to load into BigQuery.")
            raise BigQueryLoadError("No records to load into BigQuery.")

        if not self.bq_table:
            raise BigQueryLoadError("BigQuery table name is not specified")

        # Add partition decorator to table name to overwrite only that partition
        partition_date = self.partition_dt.strftime('%Y%m%d')
        partitioned_table = f"{self.bq_table}${partition_date}"

        self.logger.info(f"Loading {len(records)} records to BigQuery table: {partitioned_table}")

        # Prepare records with id and created_at fields
        all_records = []
        for record in records:
            all_records.append({
                "id": str(uuid.uuid4()),
                "raw_data": clean_nan_values(record),
                "created_at": self.partition_dt.isoformat()
            })

        # Configure BigQuery load job
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

        # Write records to temporary NDJSON file
        ndjson_path = None
        try:
            with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as ndjson_file:
                for record in all_records:
                    ndjson_file.write(json.dumps(record, cls=NaNEncoder, default=str) + '\n')
                ndjson_path = ndjson_file.name

            # Load to BigQuery
            with open(ndjson_path, 'rb') as source_file:
                load_job = self.bq_client.load_table_from_file(
                    source_file, partitioned_table, job_config=job_config
                )
            load_job.result()

            if load_job.errors:
                self.logger.error(f"BigQuery load job failed: {load_job.errors}")
                raise BigQueryLoadError(f"BigQuery load job failed: {load_job.errors}")
            else:
                self.logger.info(f"Successfully loaded data to {partitioned_table}. Job ID: {load_job.job_id}")
                return True

        except Exception as e:
            self.logger.error(f"Failed to load data to BigQuery: {str(e)}")
            raise BigQueryLoadError(f"Failed to load data to BigQuery: {str(e)}")
        finally:
            # Clean up temporary file
            if ndjson_path and os.path.exists(ndjson_path):
                os.remove(ndjson_path)

    def run(self) -> bool:
        """
        Execute the complete process: fetch data from API and load to BigQuery.

        Returns:
            bool: True if all steps completed successfully

        Raises:
            APILoaderError: If any step in the process fails
        """
        try:
            records = self.fetch_data()
            return self.load_to_bigquery(records)
        except APILoaderError as e:
            self.logger.error(f"Load process failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in load process: {str(e)}")
            raise APILoaderError(f"Unexpected error in load process: {str(e)}")
