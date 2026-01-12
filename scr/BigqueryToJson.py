import json
from datetime import datetime
import os
import tempfile
from config.cred.enviroment import Environment
from typing import Optional, List
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

    def _split_table_by_size(self, table: pa.Table, max_bytes: int) -> List[pa.Table]:
        """Split a table into multiple tables so each is roughly <= max_bytes."""
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")

        total_size = table.nbytes
        if total_size <= max_bytes or table.num_rows == 0:
            return [table]

        batches = table.to_batches(max_chunksize=10_000)
        tables: List[pa.Table] = []
        current_batches = []
        current_size = 0

        for batch in batches:
            batch_size = batch.nbytes
            if current_batches and current_size + batch_size > max_bytes:
                tables.append(pa.Table.from_batches(current_batches, schema=table.schema))
                current_batches = [batch]
                current_size = batch_size
            else:
                current_batches.append(batch)
                current_size += batch_size

            # If a single batch exceeds max_bytes, flush immediately
            if batch_size > max_bytes:
                self.logger.warning(
                    "Single batch size %s bytes exceeds max limit %s bytes; file will exceed limit.",
                    batch_size,
                    max_bytes,
                )
                tables.append(pa.Table.from_batches([batch], schema=table.schema))
                current_batches = []
                current_size = 0

        if current_batches:
            tables.append(pa.Table.from_batches(current_batches, schema=table.schema))

        self.logger.info(
            "Split table of size %.2f MB into %d chunk(s) with max %.2f MB each.",
            total_size / (1024 * 1024),
            len(tables),
            max_bytes / (1024 * 1024),
        )
        return tables

    def _write_table_to_parquet(
        self,
        table: pa.Table,
        path: Path,
        compression: str,
        use_compliant_nested_type: bool,
    ) -> None:
        """Write a pyarrow Table to parquet respecting nested type format."""
        with pq.ParquetWriter(
            str(path),
            table.schema,
            compression=compression,
            use_compliant_nested_type=use_compliant_nested_type,
        ) as writer:
            writer.write_table(table)

    def export_to_parquet(
        self,
        query: str,
        bq_table_addres: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        compression: str = 'snappy',
        use_compliant_nested_type: bool = True,
        max_parquet_size_bytes: Optional[int] = None,
    ) -> str | List[str]:
        """
        Execute query and write a Parquet file in the exporter temp dir.

        Args:
            query: SQL text
            bq_table_addres: Optional basename for parquet; defaults to generated name
            schema: Optional pyarrow.Schema to align/cast before write
            compression: Parquet compression, default 'snappy'
            use_compliant_nested_type: If False, uses new format with "<item>" for list items.
                                       If True (default), uses legacy format with "<element>" for list items.
                                       Set explicitly to ensure consistency across exports.
            max_parquet_size_bytes: Split output into multiple files so each parquet is roughly below this size.
        Returns:
            Absolute path to the written .parquet file, or list of paths if multiple files are produced.
        """
        self.logger.info(
            "Starting Parquet export with compression=%s, use_compliant_nested_type=%s, max_size=%s",
            compression,
            use_compliant_nested_type,
            max_parquet_size_bytes,
        )
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
        safe_table_name = (bq_table_addres or "query").replace("`", "").replace(".", "_")
        base_prefix = Path(self.temp_dir) / f"export_{safe_table_name}_{ts}"

        tables_to_write: List[pa.Table]
        if max_parquet_size_bytes:
            tables_to_write = self._split_table_by_size(table, max_parquet_size_bytes)
        else:
            tables_to_write = [table]

        parquet_paths: List[Path] = []
        for idx, chunk_table in enumerate(tables_to_write, start=1):
            if len(tables_to_write) == 1:
                parquet_path = base_prefix.with_suffix(".parquet")
            else:
                parquet_path = base_prefix.parent / f"{base_prefix.name}_part{idx:02d}.parquet"
            try:
                self._write_table_to_parquet(
                    chunk_table,
                    parquet_path,
                    compression=compression,
                    use_compliant_nested_type=use_compliant_nested_type,
                )
            except Exception as e:
                self.logger.warning(
                    "Could not set nested type format (%s), using default write_table for %s",
                    e,
                    parquet_path,
                )
                pq.write_table(chunk_table, str(parquet_path), compression=compression)
            parquet_paths.append(parquet_path)

        # Log schema from first file for reference
        if parquet_paths:
            curent_file = pq.read_table(parquet_paths[0])
            print("===File schema===", curent_file.schema, sep='\n')
        self.logger.info("Parquet file(s) written successfully: %s", [str(p) for p in parquet_paths])

        if len(parquet_paths) == 1:
            return str(parquet_paths[0])
        return [str(p) for p in parquet_paths]

    def export_to_parquet_gzip(
        self,
        query: str,
        bq_table_addres: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        compression: str = 'snappy',
        use_compliant_nested_type: bool = True,
        max_parquet_size_bytes: Optional[int] = None,
    ) -> str | List[str]:
        """
        Execute query and write a gzipped Parquet file (.parquet.gz) in the exporter temp dir.

        Args:
            query: SQL query to execute
            bq_table_addres: Optional basename for parquet
            schema: Optional pyarrow.Schema to align/cast before write
            compression: Parquet compression, default 'snappy'
            use_compliant_nested_type: If False, uses new format with "<item>" for list items.
                                       If True (default), uses legacy format with "<element>" for list items.
            max_parquet_size_bytes: Split parquet output before gzipping if provided.
        Returns:
            Path (or list of paths) to the .parquet.gz file(s).
        """
        self.logger.info(f"Starting Parquet.gz export for table: {bq_table_addres}")
        
        try:
            # Export to Parquet first
            parquet_paths = self.export_to_parquet(
                query,
                bq_table_addres=bq_table_addres,
                schema=schema,
                compression=compression,
                use_compliant_nested_type=use_compliant_nested_type,
                max_parquet_size_bytes=max_parquet_size_bytes,
            )
            if isinstance(parquet_paths, str):
                parquet_paths = [parquet_paths]
            self.logger.info(f"Parquet file(s) created: {parquet_paths}")
            
            # Create gzipped version
            gz_paths: List[str] = []
            for parquet_path in parquet_paths:
                gz_path = f"{parquet_path}.gz"
                self.logger.info(f"Compressing Parquet file to: {gz_path}")
                
                with open(parquet_path, 'rb') as src, gzip.open(gz_path, 'wb') as dst:
                    dst.writelines(src)
                
                # Get file sizes for logging
                parquet_size = os.path.getsize(parquet_path)
                gz_size = os.path.getsize(gz_path)
                compression_ratio = (1 - gz_size / parquet_size) * 100 if parquet_size > 0 else 0
                
                self.logger.info(
                    f"Compression completed - Original: {parquet_size:,} bytes, Compressed: {gz_size:,} bytes, Ratio: {compression_ratio:.1f}%"
                )
                self.logger.info(f"Parquet.gz file ready: {gz_path}")
                gz_paths.append(gz_path)
            
            if len(gz_paths) == 1:
                return gz_paths[0]
            return gz_paths
            
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
