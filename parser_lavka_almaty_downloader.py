from scr.ParserAPILoader import APIToBigQueryLoader
from datetime import datetime, timezone

if __name__ == '__main__':

    file_name = "yandex_lavka_almaty_mapped"
    row_table = "parser_lavka_almaty"
    loader = APIToBigQueryLoader( 
                route=f"api/csv-data/{file_name}",
                bq_table=f"organic-reef-315010.parser.row_{row_table}",
                partition_dt=datetime.now(timezone.utc)
                )

    loader.run()
    print("Data loaded to BigQuery successfully")