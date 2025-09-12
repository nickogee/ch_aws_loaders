
from scr.BigqueryShcemaToPyarrow import bq_schema_to_pyarrow
from config.cred.enviroment import Environment

client = Environment().bq_client
table = client.get_table("organic-reef-315010.indrive.amplitude_event_wo_dma")
pa_schema = bq_schema_to_pyarrow(table.schema)

print(pa_schema)