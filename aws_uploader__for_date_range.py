from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone, timedelta
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
import time

pa_schema = None
pa_schema = pa.schema([
    pa.field("batch_type", pa.string()),
    pa.field("created_at", pa.timestamp('us', tz='UTC')),
    pa.field("delivery_type", pa.int64()),
    pa.field("id", pa.int64()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("max_quantity", pa.int64()),
    pa.field("ready_time", pa.timestamp('us', tz='UTC')),
    pa.field("status", pa.int64()),
    pa.field("updated_at", pa.timestamp('us', tz='UTC')),
    pa.field("version", pa.int64()),
    pa.field("warehouse_id", pa.int64()),
    pa.field("launch_id", pa.string()),
])



def process_single_date(raw_dt, bq_table_addres, s3_entity_path, pa_schema):
    """Process data for a single date"""
    with BigQueryExporter() as exporter:
        exporter.raw_dt = raw_dt
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')    
        where_condition = f"timestamp_trunc(created_at, day) = '{exporter.dt}'"

        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
        print(f"Processing date {raw_dt} - Generated query:\n{query}")
        
        parquet_gz_path = exporter.export_to_parquet_gzip(query, schema=pa_schema, bq_table_addres=bq_table_addres)
        ##############################

        if parquet_gz_path:
            dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
            dt_now_utc = datetime.now(timezone.utc)
            upl_to_aws = S3Uploader(entity_path=s3_entity_path, 
                                    dt_now=dt_now_utc, 
                                    dt_partition=dt_partition_utc,
                                    gzip_path=parquet_gz_path)
            
            rez = upl_to_aws.run()
            print(f'Date {raw_dt}: Successfully uploaded!' if rez else f'Date {raw_dt}: Upload failed!')
            return rez
        else:
            print(f'Date {raw_dt}: No data exported')
            return False


def generate_date_range(start_date_str, end_date_str):
    """Generate list of dates between start_date and end_date (inclusive)"""
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    
    return dates


if __name__ == '__main__':
    # Define date range - modify these dates as needed
    start_date = '20250701'  # Start date in YYYYMMDD format
    end_date = '20251112'    # End date in YYYYMMDD format
    
    bq_table_addres = 'organic-reef-315010.snp.batches'
    s3_entity_path = 'partner_metrics/backend/batches'
            

    # Generate schema once
    if not pa_schema:  
        pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  
        print('===== Generated schema:', pa_schema, sep='\n')
    
    # Generate date range
    date_list = generate_date_range(start_date, end_date)
    print(f"Processing dates from {start_date} to {end_date}")
    print(f"Total dates to process: {len(date_list)}")
    
    # Process each date
    success_count = 0
    for raw_dt in date_list:
        print(f"\n--- Processing date: {raw_dt} ---")
        try:
            success = process_single_date(raw_dt, bq_table_addres, s3_entity_path, pa_schema)
            
            # time.sleep(2)
            if success:
                success_count += 1
        except Exception as e:
            print(f"Error processing date {raw_dt}: {e}")
    
    print(f"\n=== Summary ===")
    print(f"Successfully processed: {success_count}/{len(date_list)} dates")
