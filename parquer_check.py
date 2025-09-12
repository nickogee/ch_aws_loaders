import pyarrow.parquet as pq

amplitude_08 = pq.read_table('temp/bigquery_export_s6p97b3r/export_organic-reef-315010.indrive.amplitude_event_wo_dma_20250912_164848.parquet')
amplitude_04 = pq.read_table('temp/amplitude_2025-09-04.parquet')
# table.schema # pa.schema([pa.field("movie", "string", False), pa.field("release_year", "int64", True)])

# Print differences between the two schemas
schema_08 = amplitude_08.schema
schema_04 = amplitude_04.schema

fields_08 = {f.name: f for f in schema_08}
fields_04 = {f.name: f for f in schema_04}

# with open('scr/result_schema.txt', 'w', encoding='utf-8') as f:
#     f.write("Schema for amplitude_08:\n")
#     for name, field in fields_08.items():
#         f.write(f"  {name}: {field.type}\n")



only_in_08 = set(fields_08) - set(fields_04)
only_in_04 = set(fields_04) - set(fields_08)
in_both = set(fields_08) & set(fields_04)

print("Fields only in amplitude_08:")
for name in sorted(only_in_08):
    print(f"  {name}: {fields_08[name].type}")

print("\nFields only in amplitude_04:")
for name in sorted(only_in_04):
    print(f"  {name}: {fields_04[name].type}")

print("\nFields with different types:")
for name in sorted(in_both):
    if fields_08[name].type != fields_04[name].type:
        print(f"  {name}: amplitude_08={fields_08[name].type}, amplitude_04={fields_04[name].type}")