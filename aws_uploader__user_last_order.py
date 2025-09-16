from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 

pa_schema = pa.schema([
    pa.field("user_id", pa.int64()),
    pa.field("last_order_id", pa.int64()),
    pa.field("customer_phone_number", pa.int64()),
    pa.field("created_dttm_utc", pa.string()),
    pa.field("warehouse_id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("city", pa.string()),
])


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20250915'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.indrive.last_order_dttm'
        s3_entity_path = 'partner_metrics/interval_metrics/user_last_order'
        
        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres)
        print(f"Generated query:\n{query}")
        
        # Export data to temporary file
        temp_json_path = exporter.export_to_json(query, bq_table_addres)
        if temp_json_path:
            print(f"Data exported to temporary file: {temp_json_path}")
        
        ##############################

        if temp_json_path:
            # Get defined schema for parquet file
            if not pa_schema:  
                pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  

            if pa_schema:
                # Convert, compress and upload temporary file to S3.
                dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
                dt_now_utc = datetime.now(timezone.utc)
                upl_to_aws = S3Uploader(entity_path=s3_entity_path, 
                                        dt_now=dt_now_utc, 
                                        pa_schema= pa_schema, 
                                        # string_like_columns=string_like_columns, 
                                        # timestamp_columns=timestamp_columns, 
                                        dt_partition=dt_partition_utc)
                
                upl_to_aws.set_json_path(path=temp_json_path)
                rez = upl_to_aws.run()

                print('Successfully uploaded!' if rez else 'Upload failed!')
                # The file will be automatically cleaned up when the context manager exits
