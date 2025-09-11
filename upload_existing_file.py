from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
from pathlib import Path


if __name__ == '__main__':

  
    raw_dt = '20250910'
    s3_entity_path = 'partner_metrics/amplitude'
    
    gzip_path_str = 'temp/bigquery_export_p3urqf7z/export_organic-reef-315010.indrive.amplitude_event_wo_dma_20250911_141500.parquet.gz'
    gzip_path = Path(gzip_path_str)
    ##############################


    # just upload temporary file to S3.
    dt_partition_utc = datetime.strptime(str(raw_dt), '%Y%m%d')
    dt_now_utc = datetime.now(timezone.utc)
    upl_to_aws = S3Uploader(entity_path=s3_entity_path, dt_now=dt_now_utc, dt_partition=dt_partition_utc)
    # upl_to_aws.set_json_path(path=temp_json_path)
    upl_to_aws.gzip_path = gzip_path
    rez = upl_to_aws.upload_file()

    print('Successfully uploaded!' if rez else 'Upload failed!')
    # The file will be automatically cleaned up when the context manager exits