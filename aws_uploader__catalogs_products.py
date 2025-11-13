from scr.AWSS3Loader import S3Uploader
from scr.BigqueryToJson import BigQueryExporter
from datetime import datetime, timezone
import pyarrow as pa
from scr.BigqueryShcemaToPyarrow import get_pyarrow_schema_from_bq
 
pa_schema = None

pa_schema = pa.schema([
    pa.field("analytical_category_id", pa.int64()),
    pa.field("brand_id", pa.int64()),
    pa.field("country", pa.string()),
    pa.field("created_at", pa.timestamp('us', tz='UTC')),
    pa.field("daily_max_quantity", pa.int64()),
    pa.field("degree_level", pa.int64()),
    pa.field("degree_value", pa.float64()),
    pa.field("depth", pa.float64()),
    pa.field("full_description", pa.string()),
    pa.field("full_description_kk", pa.string()),
    pa.field("has_brand", pa.bool_()),
    pa.field("height", pa.float64()),
    pa.field("id", pa.int64()),
    pa.field("is_active", pa.bool_()),
    pa.field("is_sample", pa.bool_()),
    pa.field("keywords", pa.string()),
    pa.field("keywords_kk", pa.string()),
    pa.field("labels", pa.string()),
    pa.field("max_quantity", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("name_kk", pa.string()),
    pa.field("name_origin", pa.string()),
    pa.field("name_short", pa.string()),
    pa.field("name_short_kk", pa.string()),
    pa.field("one_day_sale", pa.bool_()),
    pa.field("sort_weight", pa.int64()),
    pa.field("type_uuid", pa.string()),
    pa.field("uuid", pa.string()),
    pa.field("weight", pa.int64()),
    pa.field("weight_visible", pa.bool_()),
    pa.field("width", pa.float64()),
    pa.field("wine_color", pa.string()),
    pa.field("wine_sugar_content", pa.string()),
    pa.field("is_promo", pa.bool_()),
    pa.field("is_coffee", pa.bool_()),
])


if __name__ == '__main__':

    with BigQueryExporter() as exporter:
        
        exporter.raw_dt = '20251105'
        exporter.dt = datetime.strptime(str(exporter.raw_dt), '%Y%m%d').strftime('%Y-%m-%d')
        
        bq_table_addres = 'organic-reef-315010.indrive.indrive__catalogs_products'
        s3_entity_path = 'partner_metrics/catalogs/products'
        
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
