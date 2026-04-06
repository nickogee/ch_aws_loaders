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
    pa.field("month", pa.string()),
    pa.field("active_darkstores", pa.int64()),
    pa.field("unique_users", pa.int64()),
    pa.field("orders", pa.int64()),
    pa.field("aov", pa.float64()),
    pa.field("gmv", pa.float64()),
    pa.field("delivery_fee_revenue", pa.float64()),
    pa.field("service_fee_revenue", pa.float64()),
    pa.field("gtv", pa.float64()),
        ])

query = """ 
with ord as (

SELECT
  mart_orders.platform AS platform,
  FORMAT_TIMESTAMP('%Y.%m.01', max( mart_orders.created_at)) AS month,
  COUNT(DISTINCT mart_orders.warehouse_id) AS active_darkstores,
  COUNT(DISTINCT mart_orders.user_id) AS unique_users,
  COUNT(DISTINCT mart_orders.order_id) AS orders,
  ROUND(AVG(mart_orders.original_price), 2) AS aov,
  SUM(mart_orders.original_price) AS gmv,
  SUM(mart_orders.delivery_fee_price) AS delivery_fee_revenue,
  SUM(mart_orders.service_fee_price) AS service_fee_revenue,
FROM
  `@bq_table_addres@` AS mart_orders

WHERE TIMESTAMP_TRUNC(mart_orders.created_at, month) = TIMESTAMP('@dt@')
  AND mart_orders.status <> 8

  group by mart_orders.platform
)

SELECT
  *,
  gmv + delivery_fee_revenue + service_fee_revenue as gtv,
  from ord
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


def get_current_month_start(start_date_str):
    """Get current month start date (YYYYMM01) based on start_date (YYYYMMDD)."""
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    current_month_start = datetime(start_date.year, start_date.month, 1)
    return current_month_start.strftime('%Y%m01')


if __name__ == '__main__':

    # # Get end date from BigQuery
    # with BigQueryExporter() as exporter:  
    #     # exporter.raw_dt = '20251117'
    #     end_date = exporter.raw_dt

    # Get current date in YYYYMMDD format if not set elsewhere
    start_date = '20250101'    # Start date in YYYYMMDD format
    
    print(f"Start date: {start_date}")
    
    bq_table_addres = 'organic-reef-315010.mart.mart_orders'
    s3_entity_path = 'partner_metrics/financial_aggregate/monthly'
                                                        
    # Generate schema once
    if not pa_schema:  
        pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  
        print('===== Generated schema:', pa_schema, sep='\n')
    
   # Query only current month of start_date
    current_month_dt = get_current_month_start(start_date)
    print(f"Processing only current month: {current_month_dt}")
    
    # Process current month
    try:
        success = process_single_date(current_month_dt, bq_table_addres, s3_entity_path, pa_schema)
    except Exception as e:
        print(f"Error processing current month: {e}")
        success = False
    
    if success:
        print(f"Successfully processed: {success}")
    else:
        print(f"Failed to process current month")
