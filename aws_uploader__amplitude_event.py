from scr.AWSS3Loader import S3Uploader
# from scr.AWSS3Loader import S3Uploader as S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 
pa_schema = None
pa_schema = pa.schema([
    pa.field("adid", pa.float64()),
    # pa.field("amplitude_attribution_ids", pa.list_(pa.null())),
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

string_like_columns = [
    "data",
    "event_properties",
    "group_properties",
    "groups",
    "user_properties",
        ]

timestamp_columns = [
    "client_event_time",
    "client_upload_time",
    "event_time",
    "processed_time",
    "server_received_time",
    "server_upload_time",
    "user_creation_time",
        ]

if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20251002'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        # bq_table_addres = 'organic-reef-315010.amplitude_ryadom_user_app.EVENTS_296820'
        # quickfix RDA-544
        bq_table_addres = 'organic-reef-315010.indrive.amplitude_event_wo_dma'
        s3_entity_path = 'partner_metrics/amplitude'
        where_condition = f"(lower(json_extract_scalar(event_properties,'$.user_agent')) like '%indrive%' or json_extract_scalar(event_properties, '$.currentApp' ) ='miniApp_inDrive') AND timestamp_trunc(event_time, day) = '{exporter.dt}'"

        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
        print(f"Generated query:\n{query}")
        
        # # Export data to temporary file
        # temp_json_path = exporter.export_to_json(query, bq_table_addres)
        # if temp_json_path:
        #     print(f"Data exported to temporary file: {temp_json_path}")
       
        if not pa_schema:  
            pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  
            print('===== Generate schema:', pa_schema, sep='\n')

        parquet_gz_path = exporter.export_to_parquet_gzip(query, schema=pa_schema, bq_table_addres=bq_table_addres)
        # parquet_gz_path = 'temp/bigquery_export_p05agvmn/export_organic-reef-315010.indrive.amplitude_event_wo_dma_20250925_171228.parquet.gz'
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

        # if temp_json_path:
        
        #     if pa_schema:
        #         # Convert, compress and upload temporary file to S3.
        #         dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
        #         dt_now_utc = datetime.now(timezone.utc)
        #         upl_to_aws = S3Uploader(entity_path=s3_entity_path, 
        #                                 dt_now=dt_now_utc, 
        #                                 pa_schema= pa_schema, 
        #                                 string_like_columns=string_like_columns, 
        #                                 timestamp_columns=timestamp_columns, 
        #                                 dt_partition=dt_partition_utc)
                
        #         upl_to_aws.set_json_path(path=temp_json_path)
        #         rez = upl_to_aws.run()

        #         print('Successfully uploaded!' if rez else 'Upload failed!')
        #         # The file will be automatically cleaned up when the context manager exits
