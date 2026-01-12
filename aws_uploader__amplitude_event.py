from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 
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


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20251120'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.indrive.amplitude_event_wo_dma'
        s3_entity_path = 'partner_metrics/amplitude'
        where_condition = f"(lower(json_extract_scalar(event_properties,'$.user_agent')) like '%indrive%' or json_extract_scalar(event_properties, '$.currentApp' ) ='miniApp_inDrive') AND timestamp_trunc(event_time, day) = '{exporter.dt}'"
        
        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
        print(f"Generated query:\n{query}")
        
        if not pa_schema:  
            pa_schema = get_pyarrow_schema_from_bq(table_id=bq_table_addres)  
            
        print('===== Used schema:', pa_schema, sep='\n')

        parquet_gz_paths = exporter.export_to_parquet_gzip(
            query,
            schema=pa_schema,
            bq_table_addres=bq_table_addres,
            max_parquet_size_bytes=5 * 1024 * 1024,  # 5 MB limit before gzip
        )
        ##############################

        if parquet_gz_paths:
            if isinstance(parquet_gz_paths, str):
                parquet_gz_paths = [parquet_gz_paths]

            dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
            dt_now_utc = datetime.now(timezone.utc)

            overall_success = True
            for idx, gz_path in enumerate(parquet_gz_paths):
                clear_path = idx == 0  # clear path only before first upload
                upl_to_aws = S3Uploader(
                    entity_path=s3_entity_path,
                    dt_now=dt_now_utc,
                    dt_partition=dt_partition_utc,
                    gzip_path=gz_path,
                    clear_path_before_upload=clear_path,
                )

                rez = upl_to_aws.run()
                print(f'File {idx + 1}/{len(parquet_gz_paths)} upload {"succeeded" if rez else "failed"}')
                overall_success = overall_success and rez

            print('All uploads completed successfully!' if overall_success else 'Some uploads failed!')
