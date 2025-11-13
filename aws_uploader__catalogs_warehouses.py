from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 
pa_schema = None
pa_schema = pa.schema([
    pa.field("address", pa.string()),
    pa.field("city_id", pa.int64()),
    pa.field("city_name", pa.string()),
    pa.field("closes_at", pa.time64("us")),
    pa.field("created_at", pa.timestamp('us', tz='UTC')),
    pa.field("delivery_time", pa.string()),
    pa.field("delivery_zone", pa.string()),
    pa.field("delivery_zone_geo", pa.string()),
    pa.field("delivery_zone_multi_geo", pa.string()),
    pa.field("force_min_amount", pa.int64()),
    pa.field("geo_wkt", pa.string()),
    pa.field("has_internship", pa.bool_()),
    pa.field("id", pa.int64()),
    pa.field("is_active", pa.bool_()),
    pa.field("is_functioning", pa.bool_()),
    pa.field("lat", pa.float64()),
    pa.field("long", pa.float64()),
    pa.field("min_amount", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("opens_at", pa.time64("us")),
    pa.field("products_quantity", pa.int64()),
    pa.field("uuid", pa.string()),
    pa.field("night_delivery_zone", pa.string()),
    pa.field("night_delivery_zone_multi_geo", pa.string()),
    pa.field("night_delivery_starts_at", pa.time64("us")),
    pa.field("night_delivery_ends_at", pa.time64("us")),
])


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20251105'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.indrive.indrive__catalogs_warehouses'
        s3_entity_path = 'partner_metrics/catalogs/warehouses'
        # where_condition = f"timestamp_trunc(order_creation_time, day) = '{exporter.dt}'"

        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres)
        print(f"Generated query:\n{query}")
        
        if not pa_schema:  
            pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  
            
        print('===== Used schema:', pa_schema, sep='\n')

        parquet_gz_path = exporter.export_to_parquet_gzip(query, schema=pa_schema, bq_table_addres=bq_table_addres)
        # parquet_gz_path = 'temp/bigquery_export_vbdgm_f5/export_organic-reef-315010.indrive_dev.indrive__backend_events_order_delivered_20251014_161731.parquet.gz'
        ##############################

        if parquet_gz_path:
            dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
            dt_now_utc = datetime.now(timezone.utc)
            upl_to_aws = S3Uploader(entity_path=s3_entity_path, 
                                    dt_now=dt_now_utc, 
                                    dt_partition=dt_partition_utc,
                                    gzip_path=parquet_gz_path)
            
            rez = upl_to_aws.run()
            print('Successfully uploaded!' if rez else 'Upload failed!')
            # The file will be automatically cleaned up when the context manager exits
