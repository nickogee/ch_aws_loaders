from typing import Iterable, Optional
from config.cred.enviroment import Environment


# try:
#     from google.cloud import bigquery  # type: ignore
# except Exception:  # pragma: no cover
#     bigquery = None  # Allows type-free usage when google-cloud-bigquery isn't installed

import pyarrow as pa


def _map_bq_type_to_pa(bq_type: str, precision: Optional[int] = None, scale: Optional[int] = None) -> pa.DataType:
    t = (bq_type or "").upper()

    if t in ("STRING",):
        return pa.string()
    if t in ("BYTES",):
        return pa.binary()
    if t in ("INTEGER", "INT64"):
        return pa.int64()
    if t in ("FLOAT", "FLOAT64"):
        return pa.float64()
    if t in ("BOOLEAN", "BOOL"):
        return pa.bool_()
    if t == "TIMESTAMP":
        # BigQuery TIMESTAMP is UTC; store with tz for clarity
        return pa.timestamp("us", tz="UTC")
    if t == "DATE":
        return pa.date32()
    if t == "TIME":
        return pa.time64("us")
    if t == "DATETIME":
        # No timezone info in BigQuery DATETIME
        return pa.timestamp("us")
    if t == "GEOGRAPHY":
        # Store as WKT/WKB string representation
        return pa.string()
    if t == "JSON":
        # Parquet has no native JSON; store as string
        return pa.string()
    if t == "NUMERIC":
        # BigQuery NUMERIC: precision=38, scale=9
        return pa.decimal128(precision or 38, scale or 9)
    if t == "BIGNUMERIC":
        # BigQuery BIGNUMERIC: precision up to 76, scale up to 38
        return pa.decimal256(precision or 76, scale or 38)

    # Fallback to string if an unknown type appears
    return pa.string()


def _bq_field_to_pa_field(bq_field) -> pa.Field:
    # Handle nested RECORD/STRUCT
    bq_type = getattr(bq_field, "field_type", getattr(bq_field, "type", None))
    mode = getattr(bq_field, "mode", "NULLABLE") or "NULLABLE"

    if (bq_type or "").upper() in ("RECORD", "STRUCT"):
        subfields = getattr(bq_field, "fields", [])
        struct_fields = [
            _bq_field_to_pa_field(sf) for sf in subfields
        ]
        value_type = pa.struct(struct_fields)
    else:
        precision = getattr(bq_field, "precision", None)
        scale = getattr(bq_field, "scale", None)
        value_type = _map_bq_type_to_pa(bq_type, precision=precision, scale=scale)

    # Repeated fields become lists
    if mode.upper() == "REPEATED":
        value_type = pa.list_(value_type)
        nullable = True  # Container itself can be null
    else:
        nullable = (mode.upper() != "REQUIRED")

    name = getattr(bq_field, "name", "")
    return pa.field(name, value_type, nullable=nullable)


def bq_schema_to_pyarrow(bq_schema: Iterable) -> pa.Schema:
    """
    Convert a BigQuery schema (iterable of SchemaField) to a PyArrow Schema.

    Args:
        bq_schema: Iterable of google.cloud.bigquery.SchemaField
    Returns:
        pyarrow.Schema
    """
    pa_fields = [_bq_field_to_pa_field(f) for f in bq_schema]
    return pa.schema(pa_fields)


def get_pyarrow_schema_from_bq(table_id: str) -> pa.Schema | None:
    """
    Fetch a BigQuery table schema and convert it to PyArrow.

    Args:
        table_id: Fully-qualified table id like "project.dataset.table"
    Returns:
        pyarrow.Schema
    """

    client = Environment().bq_client
    table = client.get_table(table_id)
    if client is None:
        raise RuntimeError("google-cloud-bigquery is not installed; provide a client explicitly")
    else:
        table = client.get_table(table_id)
        return bq_schema_to_pyarrow(table.schema) 