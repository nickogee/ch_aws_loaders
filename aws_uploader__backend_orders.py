from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 
pa_schema = None
pa_schema = pa.schema([
    pa.field("address", pa.string()),
    pa.field("address_comment", pa.string()),
    pa.field("auto_finish_at", pa.timestamp("us", tz="UTC")),
    pa.field("batch_id", pa.int64()),
    pa.field("building", pa.string()),
    pa.field("cell", pa.string()),
    pa.field("client_id", pa.int64()),
    pa.field("client_latitude", pa.float64()),
    pa.field("client_longitude", pa.float64()),
    pa.field("code", pa.string()),
    pa.field("comment_for_courier", pa.string()),
    pa.field("comment_for_picker", pa.string()),
    pa.field("courier_id", pa.int64()),
    pa.field("courier_motivation_amount", pa.int64()),
    pa.field("courier_motivation_time", pa.string()),
    pa.field("created_at", pa.timestamp("us", tz="UTC")),
    pa.field("delivered_time", pa.string()),
    pa.field("delivery_time_prediction", pa.string()),
    pa.field("distance_to_warehouse", pa.int64()),
    pa.field("entrance", pa.string()),
    pa.field("flat", pa.string()),
    pa.field("floor", pa.int64()),
    pa.field("has_alcohol", pa.bool_()),
    pa.field("has_coffee", pa.bool_()),
    pa.field("has_tobacco", pa.bool_()),
    pa.field("order_id", pa.int64()),
    pa.field("is_distant", pa.bool_()),
    pa.field("leave_at_the_door", pa.bool_()),
    pa.field("less_packages", pa.bool_()),
    pa.field("min_amount", pa.int64()),
    pa.field("courier_order_id", pa.int64()),
    pa.field("order_items", 
        pa.struct([
            pa.field("is_available", pa.bool_()),
            pa.field("mark", pa.string()),
            pa.field("price", pa.int64()),
            pa.field("original_price", pa.int64()),
            pa.field("product_id", pa.int64()),
            pa.field("product_name", pa.string()),
            pa.field("reason", pa.string()),
            pa.field("reason_type", pa.int64()),
            pa.field("refund_id", pa.int64()),
            pa.field("return_request_id", pa.int64()),
            pa.field("scanned_at", pa.timestamp("us", tz="UTC")),
            pa.field("status", pa.int64()),
            pa.field("supplier_price", pa.int64()),
            pa.field("supplier_product_id", pa.int64()),
            pa.field("promo_source", pa.int64()),
            pa.field("promo_source_id", pa.int64()),
        ])
    ),
    pa.field("payment_method", pa.string()),
    pa.field("picker_assigned_at", pa.timestamp("us", tz="UTC")),
    pa.field("price", pa.int64()),
    pa.field("status", pa.string()),
    pa.field("status_updated_at", pa.timestamp("us", tz="UTC")),
    pa.field("street_name", pa.string()),
    pa.field("version", pa.int64()),
    pa.field("volume", pa.int64()),
    pa.field("warehouse_address", pa.string()),
    pa.field("warehouse_id", pa.int64()),
    pa.field("warehouse_latitude", pa.string()),
    pa.field("warehouse_longitude", pa.string()),
    pa.field("weight", pa.float64()),
    pa.field("has_energy_drinks", pa.bool_()),
    pa.field("distance_to_warehouse_by_bicycle", pa.int64()),
])


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20251117'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.indrive.indrive__backend_orders'
        s3_entity_path = 'partner_metrics/backend/orders'
        where_condition = f"timestamp_trunc(created_at, day) = '{exporter.dt}'"

        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
        print(f"Generated query:\n{query}")
        
        if not pa_schema:  
            pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  
            print('===== Generate schema:', pa_schema, sep='\n')

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
