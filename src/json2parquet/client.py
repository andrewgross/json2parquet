# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime
import json

import ciso8601
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


epoch = datetime.datetime.utcfromtimestamp(0)


def ingest_data(data, schema=None, date_format=None, field_aliases=None):
    """
    data: Array of Dictionary objects
    schema: PyArrow schema object or list of column names
    date_format: Pandas datetime format string (with schema only)
    field_aliases: dict mapping Json field names to desired schema names

    return: a PyArrow Batch
    """
    if isinstance(schema, list) and isinstance(field_aliases, dict):
        return _convert_data_with_column_names_dict(data, field_aliases)
    elif isinstance(schema, dict):
        return _convert_data_with_column_names_dict(data, schema)
    elif isinstance(schema, list):
        return _convert_data_with_column_names(data, schema)
    elif isinstance(schema, pa.Schema):
        return _convert_data_with_schema(data, schema, date_format=date_format, field_aliases=field_aliases)
    else:
        return _convert_data_without_schema(data)


def _convert_data_without_schema(data):
    # Prepare for something ugly.
    # Iterate over all of the data to find all of our column names
    # Then parse the data as if we were given column names
    column_names = set()
    for row in data:
        names = set(row.keys())
        column_names = column_names.union(names)
    column_names = sorted(list(column_names))
    return _convert_data_with_column_names(data, column_names)


def _convert_data_with_column_names_dict(data, schema):
    column_data = {}
    array_data = []
    schema_names = []
    for row in data:
        for column in schema:
            _col = column_data.get(column, [])
            _col.append(row.get(column))
            column_data[column] = _col
    for column in schema.keys():
        _col = column_data.get(column)
        array_data.append(pa.array(_col))
        # Use custom column names given by user
        schema_names.append(schema[column])
    return pa.RecordBatch.from_arrays(array_data, schema_names)


def _convert_data_with_column_names(data, schema):
    column_data = {}
    array_data = []
    for row in data:
        for column in schema:
            _col = column_data.get(column, [])
            _col.append(row.get(column))
            column_data[column] = _col
    for column in schema:
        _col = column_data.get(column)
        array_data.append(pa.array(_col))
    return pa.RecordBatch.from_arrays(array_data, schema)


def _convert_data_with_schema(data, schema, date_format=None, field_aliases=None):
    column_data = {}
    array_data = []
    schema_names = []
    for row in data:
        for column in schema.names:
            _col = column_data.get(column, [])
            _col.append(row.get(column))
            column_data[column] = _col
    for column in schema:
        _col = column_data.get(column.name)
        if isinstance(column.type, pa.lib.TimestampType):
            _converted_col = []
            for t in _col:
                try:
                    _converted_col.append(pd.to_datetime(t, format=date_format))
                except pd._libs.tslib.OutOfBoundsDatetime:
                    _converted_col.append(pd.Timestamp.max)
            array_data.append(pa.Array.from_pandas(pd.to_datetime(_converted_col), type=pa.timestamp('ns')))
        elif column.type.id == pa.date32().id:
            _converted_col = map(_date_converter, _col)
            array_data.append(pa.array(_converted_col, type=pa.date32()))
        # Float types are ambiguous for conversions, need to specify the exact type
        elif column.type.id == pa.float64().id:
            array_data.append(pa.array(_col, type=pa.float64()))
        elif column.type.id == pa.float32().id:
            # Python doesn't have a native float32 type
            # and PyArrow cannot cast float64 -> float32
            _col = pd.to_numeric(_col, downcast='float')
            array_data.append(pa.Array.from_pandas(_col, type=pa.float32()))
        elif column.type.id == pa.int32().id:
            # PyArrow 0.8.0 can cast int64 -> int32
            _col64 = pa.array(_col, type=pa.int64())
            array_data.append(_col64.cast(pa.int32()))
        elif column.type.id == pa.bool_().id:
            _col = map(_boolean_converter, _col)
            array_data.append(pa.array(_col, type=column.type))
        else:
            array_data.append(pa.array(_col, type=column.type))
        if isinstance(field_aliases, dict):
            schema_names.append(field_aliases.get(column.name, column.name))
        else:
            schema_names.append(column.name)
    return pa.RecordBatch.from_arrays(array_data, schema_names)


def _boolean_converter(val):
    if val is None:
        return val
    return bool(val)


def _date_converter(date_str):
    dt = ciso8601.parse_datetime(date_str)
    return (dt - epoch).days


def load_json(filename, schema=None, date_format=None, field_aliases=None):
    """
    Simple but inefficient way to load data from a newline delineated json file
    """
    json_data = []
    with open(filename, "r") as f:
        for line in f.readlines():
            if line:
                json_data.append(json.loads(line))
    return ingest_data(json_data, schema=schema, date_format=date_format, field_aliases=field_aliases)


def write_parquet(data, destination, **kwargs):
    """
    data: PyArrow record batch
    destination: Output file name

    **kwargs: defined at https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html
    """
    try:
        table = pa.Table.from_batches(data)
    except TypeError:
        table = pa.Table.from_batches([data])
    pq.write_table(table, destination, **kwargs)


def write_parquet_dataset(data, destination, **kwargs):
    """
    data: PyArrow record batch
    destination: Output directory

    **kwargs: defined at https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html

    This adds support for writing with partitions, compared with 'write_table'.
    """
    try:
        table = pa.Table.from_batches(data)
    except TypeError:
        table = pa.Table.from_batches([data])
    pq.write_to_dataset(table, destination, **kwargs)


def convert_json(input, output, schema=None, date_format=None, field_aliases=None, **kwargs):
    data = load_json(input, schema=schema, date_format=date_format, field_aliases=field_aliases)
    write_parquet(data, output, **kwargs)
