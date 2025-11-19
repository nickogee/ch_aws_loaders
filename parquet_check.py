import pyarrow.parquet as pq

amplitude_08 = pq.read_table('temp/cancelled_orders_2025-11-15.parquet')
amplitude_04 = pq.read_table('temp/bigquery_export_1fu21_a5/export_organic-reef-315010.indrive.indrive__backend_events_cancelled_orders_20251118_094243.parquet')

# Print differences between the two schemas
schema_08 = amplitude_08.schema
schema_04 = amplitude_04.schema

fields_08 = {f.name: f for f in schema_08}
fields_04 = {f.name: f for f in schema_04}

# with open('temp/delivered_orders.txt', 'w', encoding='utf-8') as f:
#     for name, field in fields_08.items():
#         f.write(f"  {name}: {field.type}\n")

# with open('temp/cancelled_orders.txt', 'w', encoding='utf-8') as f:
#     for name, field in fields_04.items():
#         f.write(f"  {name}: {field.type}\n")


print("schema delivered_orders", schema_08, sep='\n')
# print("schema cancelled_orders", schema_04, sep='\n')

# only_in_08 = set(fields_08) - set(fields_04)
# only_in_04 = set(fields_04) - set(fields_08)
# in_both = set(fields_08) & set(fields_04)

# print("Fields only in amplitude_08:")
# for name in sorted(only_in_08):
#     print(f"  {name}: {fields_08[name].type}")

# print("\nFields only in amplitude_04:")
# for name in sorted(only_in_04):
#     print(f"  {name}: {fields_04[name].type}")

# print("\nFields with different types:")
# for name in sorted(in_both):
#     if fields_08[name].type != fields_04[name].type:
#         print(f"  {name}: amplitude_08={fields_08[name].type}, amplitude_04={fields_04[name].type}")