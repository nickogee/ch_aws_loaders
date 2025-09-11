# from scr.AWSS3Loader import S3Uploader
from scr.AWSS3Loader_fix import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone

if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20250910'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        # bq_table_addres = 'organic-reef-315010.amplitude_ryadom_user_app.EVENTS_296820'
        # quickfix RDA-544
        bq_table_addres = 'organic-reef-315010.indrive.amplitude_event_wo_dma'
        s3_entity_path = 'partner_metrics/amplitude'
        where_condition = f"(lower(json_extract_scalar(event_properties,'$.user_agent')) like '%indrive%' or json_extract_scalar(event_properties, '$.currentApp' ) ='miniApp_inDrive') AND timestamp_trunc(event_time, day) = '{exporter.dt}'"
        
        # Build query using schema
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
        print(f"Generated query:\n{query}")
        
        # Export data to temporary file
        temp_json_path = exporter.export_to_json(query, bq_table_addres)
        if temp_json_path:
            print(f"Data exported to temporary file: {temp_json_path}")
        
        ##############################

        if temp_json_path:
            # Convert, compress and upload temporary file to S3.
            dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
            dt_now_utc = datetime.now(timezone.utc)
            upl_to_aws = S3Uploader(entity_path=s3_entity_path, dt_now=dt_now_utc, dt_partition=dt_partition_utc)
            upl_to_aws.set_json_path(path=temp_json_path)
            rez = upl_to_aws.run()

            print('Successfully uploaded!' if rez else 'Upload failed!')
            # The file will be automatically cleaned up when the context manager exits
