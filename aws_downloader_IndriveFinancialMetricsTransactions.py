
from scr.AWSS3Loader import S3LogsToBigQueryLoader
from scr.BigqueryToJson import BigQueryImporter
from datetime import datetime


if __name__ == '__main__':

    with BigQueryImporter() as s3_importer:
        
        # start_date = s3_importer.dt
        start_date = '2025-09-25'
        s3_entity_path = 'financial_metrics/transactions/'
        bq_schema_path = 'organic-reef-315010.indrive'
        
        s3_prefix = f'{s3_entity_path}{start_date}/' 
        # table_name = s3_entity_path.replace('/', '-')[:len(s3_entity_path)-1] # S3 folder to scan for *.log.gz or *.parquet.gz
        table_name = 'financial_metrics-transactions-init-2'

        
        bq_table = f'{bq_schema_path}.{table_name}'  # BigQuery table to load into

        loader = S3LogsToBigQueryLoader()
        loader.load_to_bigquery(s3_prefix, bq_table, partition_dt=datetime.now())
        