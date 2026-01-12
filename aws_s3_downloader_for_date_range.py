from scr.AWSS3Loader import S3LogsToBigQueryLoader
from scr.BigqueryToJson import BigQueryImporter
from datetime import datetime, timedelta


def generate_date_range(start_date_str, end_date_str):
    """Generate list of dates between start_date and end_date (inclusive)
    
    Args:
        start_date_str: Start date in YYYY-MM-DD format
        end_date_str: End date in YYYY-MM-DD format
    
    Returns:
        List of date strings in YYYY-MM-DD format
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    return dates


def process_single_date(date_str, s3_entity_path, bq_schema_path, table_name):
    """Process data for a single date
    
    Args:
        date_str: Date in YYYY-MM-DD format
        s3_entity_path: S3 entity path (e.g., 'financial_metrics/transactions/')
        bq_schema_path: BigQuery schema path (e.g., 'organic-reef-315010.indrive_dev')
        table_name: BigQuery table name
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3_prefix = f'{s3_entity_path}{date_str}/'
        bq_table = f'{bq_schema_path}.{table_name}'
        
        # Parse date for partition
        partition_dt = datetime.strptime(date_str, '%Y-%m-%d')
        
        loader = S3LogsToBigQueryLoader()
        loader.load_to_bigquery(s3_prefix, bq_table, partition_dt=partition_dt)
        
        print(f'Date {date_str}: Successfully loaded!')
        return True
    except Exception as e:
        print(f'Date {date_str}: Error - {e}')
        return False


if __name__ == '__main__':
    with BigQueryImporter() as s3_importer:
        # Define date range - modify these dates as needed
        start_date = '2025-07-01'  # Start date in YYYY-MM-DD format
        end_date = '2025-11-30'     # End date in YYYY-MM-DD format
        
        s3_entity_path = 'financial_metrics/transactions/'
        bq_schema_path = 'organic-reef-315010.indrive'
        table_name = 'financial_metrics-transactions'
        
        # Generate date range
        date_list = generate_date_range(start_date, end_date)
        print(f"Processing dates from {start_date} to {end_date}")
        print(f"Total dates to process: {len(date_list)}")
        
        # Process each date
        success_count = 0
        for date_str in date_list:
            print(f"\n--- Processing date: {date_str} ---")
            success = process_single_date(date_str, s3_entity_path, bq_schema_path, table_name)
            if success:
                success_count += 1
        
        print(f"\n=== Summary ===")
        print(f"Successfully processed: {success_count}/{len(date_list)} dates")

