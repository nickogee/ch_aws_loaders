import functions_framework
from google.cloud import bigquery, storage
import xml.etree.ElementTree as ET
from datetime import datetime

# Ваша функция (добавлен возврат XML-строки)
def create_xml_from_data(data: list) -> bytes:
    if not data:
        return b""
    
    current_date = datetime.now()
    yml_catalog = ET.Element('yml_catalog', {'date': str(current_date.isoformat())})
    shop = ET.SubElement(yml_catalog, 'shop')
    
    # Заполнение данными (сокращено для краткости, логика та же)
    ET.SubElement(shop, 'name').text = data[0].get('mercant_name', '')
    ET.SubElement(shop, 'company').text = data[0].get('mercant_id', '')
    ET.SubElement(shop, 'categories')
    offers = ET.SubElement(shop, 'offers')
    
    for offer_dct in data:
        offer = ET.SubElement(offers, 'offer', {'id': str(offer_dct['id'])})
        ET.SubElement(offer, 'name').text = str(offer_dct.get('title', ''))
        ET.SubElement(offer, 'price').text = str(int(offer_dct.get('price', 0)))
        # ... добавьте остальные поля из вашего шаблона ...

    return ET.tostring(yml_catalog, encoding='utf-8', xml_declaration=True)

@functions_framework.http
def bq_to_xml_gcs(request):
    bq_client = bigquery.Client()
    storage_client = storage.Client()

    # 1. Получение данных из BigQuery
    query = "SELECT * FROM `your_project.dataset.table` LIMIT 1000"
    query_job = bq_client.query(query)
    # Превращаем результат в список словарей
    data = [dict(row) for row in query_job.result()]

    if not data:
        return "No data found", 204

    # 2. Генерация XML
    xml_bytes = create_xml_from_data(data)

    # 3. Загрузка в Cloud Storage
    bucket_name = "your-bucket-name"
    destination_blob_name = f"exports/products_{datetime.now().strftime('%Y%m%d')}.xml"
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_string(xml_bytes, content_type='application/xml')

    return f"Successfully uploaded to {destination_blob_name}", 200
