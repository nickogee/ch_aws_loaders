from scr.aws_s3_upload_file_cursor import S3Uploader
from scr.bigquery_to_json import BigQueryExporter
from datetime import datetime, timezone


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        # Set params to get data
        bq_table_addres = 'organic-reef-315010.indrive.amplitude_event_wo_dma'
        s3_entity_path = 'partner_metrics/amplitude'
        where_condition = f"(lower(json_extract_scalar(event_properties,'$.user_agent')) like '%indrive%' or json_extract_scalar(event_properties, '$.currentApp' ) ='miniApp_inDrive') AND timestamp_trunc(event_time, day) = '{exporter.dt}'"
        
        # bq_table_addres = 'organic-reef-315010.indrive.backend_events_order_delivered'
        # s3_entity = 'backend_events/delivered_orders/'
        
        # where_condition = f"(lower(json_extract_scalar(event_properties,'$.user_agent')) like '%indrive%' or json_extract_scalar(event_properties, '$.currentApp' ) ='miniApp_inDrive') AND timestamp_trunc(event_time, day) = '2025-07-20'"
        # where_condition = "True"

        # Build query using schema
        # query = exporter.build_query(bq_table_addres)
        query = exporter.build_query(bq_table_addres=bq_table_addres, where_condition=where_condition)
        print(f"Generated query:\n{query}")
        
        # Export data to temporary file
        temp_json_path = exporter.export_to_json(query, bq_table_addres)
        print(f"Data exported to temporary file: {temp_json_path}")
        
        ##############################

        # upl_to_aws = S3Uploader(entity=s3_entity)
        # upl_to_aws.set_json_path(path=temp_json_path)
        # # upl_to_aws.set_json_path(path='scr/delivered_orders.json')
        # rez = upl_to_aws.run()

        # # print(f'Successfully uploaded - {rez}')
        # print('Successfully uploaded!' if rez else 'Upload failed!')
        # # The file will be automatically cleaned up when the context manager exits

        if temp_json_path:
            # Convert, compress and upload temporary file to S3.
            dt_partition_utc = datetime.strptime(str(exporter.raw_dt), '%Y%m%d')
            dt_now_utc = datetime.now(timezone.utc)
            upl_to_aws = S3Uploader(entity_path=s3_entity_path, dt_now=dt_now_utc, dt_partition=dt_partition_utc)
            upl_to_aws.set_json_path(path=temp_json_path)
            rez = upl_to_aws.run()

            print('Successfully uploaded!' if rez else 'Upload failed!')
            # The file will be automatically cleaned up when the context manager exits


# Example usage (e.g., in your main.py or a new script)




# from google.cloud import bigquery # For custom job_config
# import logging
# from datetime import datetime

# if __name__ == '__main__':
#     from scr.aws_s3_delete_obgects import S3PathCleaner
#     from config.cred.enviroment import Environment

#     env = Environment()
#     bucket_name = env.aws_s3_bucket_name
#     access_key = env.aws_s3_access_key
#     secret_key = env.aws_s3_secret_key
#     region_name = env.aws_s3_region_name
    
#     cleaner = S3PathCleaner(
#         bucket_name=bucket_name,
#         access_key=access_key,
#         secret_key=secret_key,
#         region_name=region_name
#     )

#     s3_path = 'partner_metrics/interval_metrics/open_user_funnel/2025-06-26/'
#     cleaner.clear_s3_path(s3_path)

# from scr.s3_logs_to_bigquery import S3LogsToBigQueryLoader
# from scr.parquet_file_to_bigquery import load_to_bigquery

# if __name__ == '__main__':
#     # start_date = '2025-06-26'
#     # s3_entity_path = 'financial_metrics/transactions/'
#     # bq_schema_path = 'organic-reef-315010.indrive'
#     # # bq_schema_path = 'organic-reef-315010.dbt_analytics_srv'
    
#     # s3_prefix = f'{s3_entity_path}{start_date}/' 
#     # table_name = s3_entity_path.replace('/', '-')[:len(s3_entity_path)-1] # S3 folder to scan for *.log.gz
    
#     # bq_table = f'{bq_schema_path}.{table_name}'  # BigQuery table to load into

#     # loader = S3LogsToBigQueryLoader()
#     # loader.load_to_bigquery(s3_prefix, bq_table, start_date)

#     bq_table = 'organic-reef-315010.dbt_analytics_srv.financial_metrics'
#     start_date = '2025-08-06'
#     parquet_path = 'financial_metrics_2025-08-06.parquet'
#     load_to_bigquery(bq_table, start_date, parquet_path)