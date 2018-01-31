# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging

from inspect import cleandoc
from urlparse import urlparse

import pyarrow as pa

logger = logging.getLogger("json2parquet")


def get_schema_from_redshift(redshift_schema, redshift_table, redshift_uri, partition_columns=None):
    """
    Creates a PyArrow Schema based on a Redshift Table, uses psycopg2
    """
    if partition_columns is None:
        partition_columns = []
    query = _get_redshift_schema(redshift_schema, redshift_table)
    raw_schema = run_redshift_query(query, redshift_uri)
    return _convert_schema(raw_schema, partition_columns)


def _convert_schema(redshift_schema, partition_columns):
    fields = []
    for column in redshift_schema:
        column_name = column[0]
        if column_name in partition_columns:
            # Allow skipping virtual columns used for partitioning
            continue
        column_type = column[1].upper()
        numeric_precision = column[2]
        numeric_scale = column[3]
        datetime_precision = column[4]
        converted_type = _convert_type(column_type, numeric_precision, numeric_scale, datetime_precision)
        fields.append(pa.field(column_name, converted_type))
    schema = pa.schema(fields)
    return schema


def _convert_type(column_type, numeric_prec, numeric_scale, datetime_prec):
    converted_type = REDSHIFT_TO_PYARROW_MAPPING[column_type]
    if column_type in ["TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"]:
        return converted_type('ns')
    elif column_type in ["TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"]:
        # We have no way to get TZ info from Redshift schema and
        # I think Redshift stores the TZ per value
        return converted_type('ns')
    elif column_type in ["DECIMAL", "NUMERIC"]:
        return converted_type(numeric_prec, numeric_scale)
    else:
        return converted_type()


def run_redshift_query(query, redshift_uri, isolation_level=1):
    import psycopg2
    logger.debug("Running query:\n{}".format(query))
    uri = urlparse(redshift_uri)
    with psycopg2.connect(
        dbname=uri.path.strip('/'),
        user=uri.username,
        password=uri.password,
        host=uri.hostname,
        port=uri.port,
    ) as conn:
        conn.set_isolation_level(isolation_level)
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = None
            try:
                result = cursor.fetchall()
            finally:
                cursor.close()
            return result


def _get_redshift_schema(table_name, schema_name):
    """
    Gets the table schema for any redshift table, Spectrum or Regular.
    Slower perf than using pg_table_def, but gets Spectrum tables
    """
    SCHEMA_QUERY = cleandoc("""
        SELECT
            column_name,
            data_type,
            numeric_precision,
            numeric_scale,
            datetime_precision
        FROM svv_columns
        WHERE table_schema = '{table_name}'
        AND table_name = '{schema_name}'
        ORDER BY column_name;
    """.format(table_name=table_name, schema_name=schema_name))
    return SCHEMA_QUERY


# https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
# We need to figure out a way to account for whatever data type Python stuffs
# JSON data into when loading it, as we might lose some precision when pushing
# it in to Parquet if we are not careful.
REDSHIFT_TO_PYARROW_MAPPING = {
    # Signed 2-byte Int
    "SMALLINT": pa.int16,
    "INT2": pa.int16,

    # Signed 4-byte Integer
    "INTEGER": pa.int32,
    "INT": pa.int32,
    "INT4": pa.int32,

    # Signed 8-byte Integer
    "BIGINT": pa.int64,
    "INT8": pa.int64,

    # Logical Boolean
    "BOOLEAN": pa.bool_,
    "BOOL": pa.bool_,

    # Single Precision Floating Point Numb
    "REAL": pa.float32,
    "FLOAT4": pa.float32,

    # Double  Precision Floating Point Numb
    "DOUBLE PRECISION": pa.float64,
    "FLOAT8": pa.float64,
    "FLOAT": pa.float64,

    # Exact numeric of selectable precision
    "DECIMAL": pa.decimal128,
    "NUMERIC": pa.decimal128,

    # Fixed Length String
    # PyArrow has no concept of fixed length strings in schemas
    "CHAR": pa.string,
    "CHARACTER": pa.string,
    "NCHAR": pa.string,
    "BPCHAR": pa.string,

    # Variable Length String
    "VARCHAR": pa.string,
    "CHARACTER VARYING": pa.string,
    "NVARCHAR": pa.string,
    "TEXT": pa.string,

    # Calender Date Y, M, D
    # Redshift spec has Date with 1 day resolution
    "DATE": pa.date32,

    # Timestamp w/o TZ info
    # Redshift spec has Timestamp at 1 microsecond resolution
    "TIMESTAMP": pa.timestamp,
    "TIMESTAMP WITHOUT TIME ZONE": pa.timestamp,

    # Timestamp with TZ
    # Need a way to pull TZ info
    # Redshift spec has Timestamp at 1 microsecond resolution
    "TIMESTAMPTZ": pa.timestamp,
    "TIMESTAMP WITH TIME ZONE": pa.timestamp,
}
