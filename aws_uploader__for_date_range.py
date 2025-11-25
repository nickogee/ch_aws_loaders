from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone, timedelta
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
import time

pa_schema = None
pa_schema = pa.schema([
    pa.field("adid", pa.float64()),
    pa.field("amplitude_attribution_ids", pa.list_(pa.string())),
    pa.field("amplitude_event_type", pa.float64()),
    pa.field("amplitude_id", pa.int64()),
    pa.field("app", pa.int64()),
    pa.field("city", pa.string()),
    pa.field("client_event_time", pa.timestamp('us', tz='UTC')),
    pa.field("client_upload_time", pa.timestamp('us', tz='UTC')),
    pa.field("country", pa.string()),
    pa.field("data", pa.string()),
    pa.field("device_brand", pa.float64()),
    pa.field("device_carrier", pa.float64()),
    pa.field("device_family", pa.string()),
    pa.field("device_id", pa.string()),
    pa.field("device_manufacturer", pa.float64()),
    pa.field("device_model", pa.float64()),
    pa.field("device_type", pa.string()),
    pa.field("event_id", pa.int64()),
    pa.field("event_properties", pa.string()),
    pa.field("event_time", pa.timestamp('us', tz='UTC')),
    pa.field("event_type", pa.string()),
    pa.field("followed_an_identify", pa.float64()),
    pa.field("group_properties", pa.string()),
    pa.field("groups", pa.string()),
    pa.field("idfa", pa.float64()),
    pa.field("ip_address", pa.string()),
    pa.field("is_attribution_event", pa.float64()),
    pa.field("language", pa.string()),
    pa.field("library", pa.string()),
    pa.field("location_lat", pa.float64()),
    pa.field("location_lng", pa.float64()),
    pa.field("os_name", pa.string()),
    pa.field("os_version", pa.string()),
    pa.field("paying", pa.float64()),
    pa.field("platform", pa.string()),
    pa.field("processed_time", pa.timestamp('us', tz='UTC')),
    pa.field("region", pa.string()),
    pa.field("sample_rate", pa.float64()),
    pa.field("server_received_time", pa.timestamp('us', tz='UTC')),
    pa.field("server_upload_time", pa.timestamp('us', tz='UTC')),
    pa.field("session_id", pa.int64()),
    pa.field("start_version", pa.float64()),
    pa.field("user_creation_time", pa.timestamp('us')),
    pa.field("user_id", pa.float64()),
    pa.field("user_properties", pa.string()),
    pa.field("uuid", pa.string()),
    pa.field("version_name", pa.float64()),
        ])




def process_single_date(raw_dt, bq_table_addres, s3_entity_path, pa_schema):
    """Process data for a single date"""
    with BigQueryExporter() as exporter:
        exporter.raw_dt = raw_dt
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')    
        
        where_condition = f"(lower(json_extract_scalar(event_properties,'$.user_agent')) like '%indrive%' or json_extract_scalar(event_properties, '$.currentApp' ) ='miniApp_inDrive') AND timestamp_trunc(event_time, day) = '{exporter.dt}'"
    
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
    start_date = '20251124'  # Start date in YYYYMMDD format
    end_date = '20251124'    # End date in YYYYMMDD format
    
    bq_table_addres = 'organic-reef-315010.indrive.amplitude_event_wo_dma'
    s3_entity_path = 'partner_metrics/amplitude'
                                                        
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
            
            time.sleep(2)
            if success:
                success_count += 1
        except Exception as e:
            print(f"Error processing date {raw_dt}: {e}")
    
    print(f"\n=== Summary ===")
    print(f"Successfully processed: {success_count}/{len(date_list)} dates")
