import json
from datetime import datetime
import os
import tempfile
from config.cred.enviroment import Environment
from typing import Optional
from pathlib import Path
import gzip
import logging

import pyarrow as pa
import pyarrow.parquet as pq


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    

# class BigQueryExporter(BQLoader):
class BigQueryExporter():
    def __init__(self):
        # super().__init__(name=None)
        self.client = Environment().bq_client
        self.env = Environment()
        self.temp_dir = None
        self._setup_logging()
        '''
        self.dt, self.dt_raw -> UTC from AirFlow bash comand parameters

        S3 path format:
        partner_metrics/entity/YYYY-MM-DD(BQ table partition date in UTC)/{str8_hash}_HH:MM:SS(upload time in UTC)
        '''
        

    def _setup_logging(self) -> None:
        """Configure logging for the exporter."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        """Create temporary directory when entering context."""
        # self.temp_dir = tempfile.mkdtemp(prefix='bigquery_export_')
        self.temp_dir = tempfile.mkdtemp(prefix='bigquery_export_', dir='/Users/hachimantaro/Repo/choco_projects/ch_indrive_aws_s3/temp/')
        self.logger.info(f"Created temporary directory: {self.temp_dir}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up temporary directory when exiting context."""
        pass
        # if self.temp_dir and os.path.exists(self.temp_dir):
        #     shutil.rmtree(self.temp_dir)

    def get_table_schema(self, bq_table_addres: str) -> list:
        """
        Get column names from BigQuery table schema.

        Args:
            bq_table_addres: BigQuery table pull path

        Returns:
            list: List of column names
        """
        try:
            self.logger.info(f"Getting schema for table: {bq_table_addres}")
            table = self.client.get_table(bq_table_addres)
            columns = [field.name for field in table.schema]
            self.logger.info(f"Found {len(columns)} columns in table schema")
            return columns
        except Exception as e:
            self.logger.error(f"Error getting table schema: {str(e)}")
            raise

    def build_query(self, bq_table_addres: str, where_condition: str = 'TRUE') -> str:
        """
        Build SQL query using table schema.

        Args:
            bq_table_addres: BigQuery table full path
            where_condition: A condition for filter data in the table

        Returns:
            str: SQL query string
        """

        columns = self.get_table_schema(bq_table_addres)
        columns_str = ", ".join(f"`{col}`" for col in columns)
        query = f"""
        SELECT {columns_str}
        FROM `{bq_table_addres}`
        WHERE {where_condition} 
        """
        self.logger.info(f"Built query with {len(columns)} columns and condition: {where_condition}")
        return query

    # Option 1: Direct BigQuery → Arrow → Parquet helpers
    def to_arrow(self, query: str) -> pa.Table:
        """Execute query and return a PyArrow Table (uses BQ Storage API if available)."""
        self.logger.info("Executing BigQuery query and converting to Arrow table")
        try:
            job = self.client.query(query)
            result = job.result()
            table = result.to_arrow()
            self.logger.info(f"Successfully converted query result to Arrow table with {table.num_rows} rows and {table.num_columns} columns")
            return table
        except Exception as e:
            self.logger.error(f"Failed to convert query result to Arrow table: {str(e)}")
            raise

    def _sanitize_schema(self, schema: pa.Schema) -> pa.Schema:
        """Replace unsupported target types (null, list<null>) with compatible types (string)."""
        self.logger.debug("Sanitizing schema to replace unsupported null types")
        new_fields = []
        for f in schema:
            target_type = f.type
            try:
                if pa.types.is_null(target_type):
                    target_type = pa.string()
                    self.logger.debug(f"Replaced null type with string for field: {f.name}")
                elif pa.types.is_list(target_type):
                    value_type = target_type.value_type
                    if pa.types.is_null(value_type):
                        target_type = pa.list_(pa.string())
                        self.logger.debug(f"Replaced list<null> with list<string> for field: {f.name}")
            except Exception:
                pass
            new_fields.append(pa.field(f.name, target_type, nullable=True))
        return pa.schema(new_fields)

    def export_to_parquet(self, query: str, bq_table_addres: Optional[str] = None, schema: Optional[pa.Schema] = None, compression: str = 'snappy') -> str:
        """
        Execute query and write a Parquet file in the exporter temp dir.

        Args:
            query: SQL text
            bq_table_addres: Optional basename for parquet; defaults to generated name
            schema: Optional pyarrow.Schema to align/cast before write
            compression: Parquet compression, default 'snappy'
        Returns:
            Absolute path to the written .parquet file
        """
        self.logger.info(f"Starting Parquet export with compression: {compression}")
        table = self.to_arrow(query)

        if schema is not None:
            self.logger.info("Applying schema alignment and type casting")
            # Sanitize schema to avoid null target types
            schema = self._sanitize_schema(schema)
            try:
                # Add missing columns as typed null arrays
                for field in schema:
                    if field.name not in table.column_names:
                        col = pa.array([None] * table.num_rows, type=field.type)
                        table = table.append_column(field.name, col)
                        self.logger.debug(f"Added missing column: {field.name}")
                # Reorder and cast
                table = table.select([f.name for f in schema])
                table = table.cast(schema, safe=False)
                self.logger.info("Schema alignment completed successfully")
            except Exception as e:
                self.logger.warning(f"Schema alignment warning: {e}")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.join(self.temp_dir, f"export_{bq_table_addres}_{ts}.parquet")
        out_path = Path(self.temp_dir) / base
        pq.write_table(table, str(out_path), compression=compression)
        self.logger.info(f"Parquet file written successfully: {out_path}")
        return str(out_path)

    def export_to_parquet_gzip(self, query: str, bq_table_addres: Optional[str] = None, schema: Optional[pa.Schema] = None, compression: str = 'snappy') -> str:
        """
        Execute query and write a gzipped Parquet file (.parquet.gz) in the exporter temp dir.

        Returns path to the .parquet.gz file.
        """
        self.logger.info(f"Starting Parquet.gz export for table: {bq_table_addres}")
        
        try:
            # Export to Parquet first
            parquet_path = self.export_to_parquet(query, bq_table_addres=bq_table_addres, schema=schema, compression=compression)
            self.logger.info(f"Parquet file created: {parquet_path}")
            
            # Create gzipped version
            gz_path = f"{parquet_path}.gz"
            self.logger.info(f"Compressing Parquet file to: {gz_path}")
            
            with open(parquet_path, 'rb') as src, gzip.open(gz_path, 'wb') as dst:
                dst.writelines(src)
            
            # Get file sizes for logging
            parquet_size = os.path.getsize(parquet_path)
            gz_size = os.path.getsize(gz_path)
            compression_ratio = (1 - gz_size / parquet_size) * 100 if parquet_size > 0 else 0
            
            self.logger.info(f"Compression completed - Original: {parquet_size:,} bytes, Compressed: {gz_size:,} bytes, Ratio: {compression_ratio:.1f}%")
            self.logger.info(f"Parquet.gz file ready: {gz_path}")
            
            return gz_path
            
        except Exception as e:
            self.logger.error(f"Failed to create Parquet.gz file: {str(e)}")
            raise

    def export_to_json(self, query: str, bq_table_addres: str) -> str:
        """
        Execute BigQuery query and save results to temporary JSON file.

        Args:
            query: SQL query to execute
            bq_table_addres: BigQuery table full path

        Returns:
            str: Path to the temporary JSON file
        """
        try:
            self.logger.info(f"Starting JSON export for table: {bq_table_addres}")
            # Execute query
            query_job = self.client.query(query)
            results = list(query_job.result())
            self.logger.info(f'Query result - {len(results)} rows')
            # Convert to list of dictionaries
            data = [dict(row) for row in results]
            if not data:
                raise Exception("No data found in query result. No file will be created.")

            # Create temporary file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_file = os.path.join(self.temp_dir, f"export_{bq_table_addres}_{timestamp}.json")

            # Save to JSON file with custom encoder for datetime
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder, default=str)

            file_size = os.path.getsize(temp_file)
            self.logger.info(f"JSON file written successfully: {temp_file} ({file_size:,} bytes)")
            return temp_file

        except Exception as e:
            self.logger.error(f"Error exporting data to JSON: {str(e)}")
            raise

# class BigQueryImporter(BQLoader):
class BigQueryImporter():
    def __init__(self):
        # super().__init__(name=None)
        self.client = Environment().bq_client
        self.env = Environment()
        self.temp_dir = None
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up temporary directory when exiting context."""
        pass
        # if self.temp_dir and os.path.exists(self.temp_dir):
        #     shutil.rmtree(self.temp_dir)
