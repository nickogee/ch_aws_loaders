from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 

pa_schema = None
pa_schema = pa.schema([
    pa.field("current_app", pa.string()),
    pa.field("session_start_superapp", pa.int64()),
    pa.field("session_start_miniapp", pa.int64()),
    pa.field("product_added", pa.int64()),
    pa.field("went_cart", pa.int64()),
    pa.field("started_payment", pa.int64()),
    pa.field("purchase", pa.int64()),
])


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20250921'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.indrive.open_user_funnel'
        s3_entity_path = 'partner_metrics/interval_metrics/open_user_funnel'
        
        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres)
        print(f"Generated query:\n{query}")
        
        # # Export data to temporary file
        # temp_json_path = exporter.export_to_json(query, bq_table_addres)
        # if temp_json_path:
        #     print(f"Data exported to temporary file: {temp_json_path}")
        
        parquet_gz_path = exporter.export_to_parquet_gzip(query, schema=pa_schema, bq_table_addres=bq_table_addres)
        
        ##############################

        # if temp_json_path:
        #     # Get defined schema for parquet file
        #     if not pa_schema:  
        #         pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  

        #     if pa_schema:
        #         # Convert, compress and upload temporary file to S3.
        #         dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
        #         dt_now_utc = datetime.now(timezone.utc)
        #         upl_to_aws = S3Uploader(entity_path=s3_entity_path, 
        #                                 dt_now=dt_now_utc, 
        #                                 pa_schema= pa_schema, 
        #                                 # string_like_columns=string_like_columns, 
        #                                 # timestamp_columns=timestamp_columns, 
        #                                 dt_partition=dt_partition_utc)
                
        #         upl_to_aws.set_json_path(path=temp_json_path)
        #         rez = upl_to_aws.run()

        #         print('Successfully uploaded!' if rez else 'Upload failed!')
        #         # The file will be automatically cleaned up when the context manager exits
