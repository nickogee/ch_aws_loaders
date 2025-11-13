from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 
pa_schema = None
pa_schema = pa.schema([
    pa.field("created_at", pa.timestamp('us', tz='UTC')),
    pa.field("created_by", pa.int64()),
    pa.field("document_id", pa.int64()),
    pa.field("document_type_id", pa.int64()),
    pa.field("feature_id", pa.int64()),
    pa.field("id", pa.int64()),
    pa.field("product_id", pa.int64()),
    pa.field("quantity", pa.int64()),
    pa.field("warehouse_id", pa.int64()),
    pa.field("launch_id", pa.string()),
    pa.field("is_deleted", pa.bool_()),
])

if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20251101'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.snp.operations'
        s3_entity_path = 'partner_metrics/backend/operations'
        where_condition = f"timestamp_trunc(created_at, day) = '{exporter.dt}'"

        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
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
