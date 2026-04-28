from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone, timedelta
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
import time
import copy

pa_schema = None
pa_schema = pa.schema([
    pa.field("platform", pa.string()),
    pa.field("created_at", pa.string()),
    pa.field("warehouse_id", pa.int64()),
    pa.field("user_id", pa.int64()),
    pa.field("order_id", pa.int64()),
    pa.field("original_price", pa.float64()),
    pa.field("gmv_disc", pa.float64()),
    pa.field("incentives", pa.float64()),
    pa.field("delivery_fee_price", pa.float64()),
    pa.field("service_fee_price", pa.float64()),
    pa.field("status", pa.int64()),
    pa.field("payment_id", pa.string()),
        ])

query = """ 
SELECT
  mart_orders.platform,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', mart_orders.created_at) as created_at,
  mart_orders.warehouse_id,
  mart_orders.user_id,
  mart_orders.order_id,
  mart_orders.original_price,
  mart_orders.oi_price as gmv_disc,
  mart_orders.original_price - mart_orders.oi_price as incentives,
  mart_orders.delivery_fee_price,
  mart_orders.service_fee_price,
  mart_orders.status,
  IFNULL(mart_orders.payment_id, "") as payment_id,
FROM
  `@bq_table_addres@` AS mart_orders

WHERE TIMESTAMP_TRUNC(mart_orders.created_at, day) = TIMESTAMP('@dt@')
"""


def process_single_date(raw_dt, bq_table_addres, s3_entity_path, pa_schema):
    """Process data for a single date"""
    global query
    current_query = copy.deepcopy(query)

    with BigQueryExporter() as exporter:
        exporter.raw_dt = raw_dt
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')    
        
        # Build query using schema
        current_query = current_query.replace("@dt@", exporter.dt)
        current_query = current_query.replace("@bq_table_addres@", bq_table_addres)
        print(f"Processing date {raw_dt} - Generated query:\n{current_query}")
        
        parquet_gz_path = exporter.export_to_parquet_gzip(current_query, schema=pa_schema, bq_table_addres=bq_table_addres)
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

    # # Get end date from BigQuery
    # with BigQueryExporter() as exporter:  
    #     # exporter.raw_dt = '20251117'
    #     end_date = exporter.raw_dt

    # Get current date in YYYYMMDD format if not set elsewhere
    start_date = '20260422'    # Start date in YYYYMMDD format
    end_date = '20260422'    # End date in YYYYMMDD format
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')    # End date in YYYYMMDD format
    
    end_date_dt = datetime.strptime(str(end_date), '%Y%m%d')
    
    # Define date range
    if start_date is None:
        start_date_dt = end_date_dt - timedelta(days=2)
        start_date = start_date_dt.strftime('%Y%m%d')  # Start date in YYYYMMDD format
    
    bq_table_addres = 'organic-reef-315010.mart.mart_orders'
    s3_entity_path = 'partner_metrics/financial_aggregate/raw_daily'
                                                        
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
    unsuccess_date_dt = {}
    for raw_dt in date_list:
        print(f"\n--- Processing date: {raw_dt} ---")
        try:
            success = process_single_date(raw_dt, bq_table_addres, s3_entity_path, pa_schema)
            
            # time.sleep(2)
            if success:
                success_count += 1
        except Exception as e:
            unsuccess_date_dt[str(raw_dt)] = str(e)
            print(f"Error processing date {raw_dt}: {e}")
    
    print(f"\n=== Summary ===")
    print(f"Successfully processed: {success_count}/{len(date_list)} dates")
    if unsuccess_date_dt:
        print(f"Unsuccessfully processed: {unsuccess_date_dt}")
        raise RuntimeError(f"Some dates failed to process: {unsuccess_date_dt.keys()}")
   
        
