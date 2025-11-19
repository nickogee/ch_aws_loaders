from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone, timedelta
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
import time

pa_schema = None
pa_schema = pa.schema([
    pa.field("name", pa.string()),
    pa.field("user_id", pa.string()),
    pa.field("city_name", pa.string()),
    pa.field("county_code", pa.string()),
    pa.field("event_id", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("actual_delivery_time", pa.timestamp('us', tz='UTC')),
    pa.field("order_creation_time", pa.timestamp('us', tz='UTC')),
    pa.field("min_promised_delivery_time", pa.int64()),
    pa.field("max_promised_delivery_time", pa.int64()),
    pa.field(
        "delivered_products",
            pa.struct([
                pa.field("name", pa.string()),
                pa.field("id", pa.int64()),
                pa.field("price", pa.int64()),
                pa.field("original_price", pa.int64()),
                pa.field("quantity", pa.int64())
            ])
        ),
    pa.field("order_id", pa.string()),
    pa.field("payment_id", pa.string()),
    pa.field("promocode_name", pa.string()),
    pa.field("promocode_conditions", pa.string()),
])




def process_single_date(raw_dt, bq_table_addres, s3_entity_path, pa_schema):
    """Process data for a single date"""
    with BigQueryExporter() as exporter:
        exporter.raw_dt = raw_dt
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')    
        
        where_condition = f"timestamp_trunc(order_creation_time, day) = '{exporter.dt}'"

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
    start_date = '20251115'  # Start date in YYYYMMDD format
    end_date = '20251117'    # End date in YYYYMMDD format
    
    bq_table_addres = 'organic-reef-315010.indrive.indrive__backend_events_order_delivered'
    s3_entity_path = 'partner_metrics/backend_events/delivered_orders'
                                                    
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
