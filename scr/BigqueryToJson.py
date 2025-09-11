import json
from datetime import datetime
import os
import tempfile
import shutil
from config.cred.enviroment import Environment
# from BQLoader import BQLoader


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
        '''
        self.dt, self.dt_raw -> UTC from AirFlow bash comand parameters

        S3 path format:
        partner_metrics/entity/YYYY-MM-DD(BQ table partition date in UTC)/{str8_hash}_HH:MM:SS(upload time in UTC)
        '''
        

    def __enter__(self):
        """Create temporary directory when entering context."""
        # self.temp_dir = tempfile.mkdtemp(prefix='bigquery_export_')
        self.temp_dir = tempfile.mkdtemp(prefix='bigquery_export_', dir='/Users/hachimantaro/Repo/choco_projects/ch_indrive_aws_s3/temp/')
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
            table = self.client.get_table(bq_table_addres)
            return [field.name for field in table.schema]
        except Exception as e:
            print(f"Error getting table schema: {str(e)}")
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
        return f"""
        SELECT {columns_str}
        FROM `{bq_table_addres}`
        WHERE {where_condition} 
        """

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
            # Execute query
            query_job = self.client.query(query)
            results = list(query_job.result())
            print(f'---- Query result - {len(results)} rows')
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


            print(f"Data successfully exported to temporary file: {temp_file}")
            return temp_file

        except Exception as e:
            print(f"Error exporting data: {str(e)}")
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