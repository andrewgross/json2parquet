# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def ingest_data(data, schema=None, date_format=None):
    """
    data: Array of Dictionary objects
    schema: PyArrow schema object or list of column names
    date_format: Pandas datetime format string (with schema only)

    return: a PyArrow Batch
    """
    if isinstance(schema, list):
        return _convert_data_with_column_names(data, schema)
    elif isinstance(schema, pa.Schema):
        return _convert_data_with_schema(data, schema, date_format=date_format)
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


def _convert_data_with_schema(data, schema, date_format=None):
    column_data = {}
    array_data = []
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
        else:
            array_data.append(pa.array(_col, type=column.type))
    return pa.RecordBatch.from_arrays(array_data, schema.names)


def load_json(filename, schema, date_format=None):
    """
    Simple but inefficient way to load data from a newline delineated json file
    """
    json_data = []
    with open(filename, "r") as f:
        for line in f.readlines():
            if line:
                json_data.append(json.loads(line))
    return ingest_data(json_data, schema, date_format=date_format)


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


def convert_json(input, output, schema, date_format=None, **kwargs):
    data = load_json(input, schema, date_format=date_format)
    write_parquet(data, output, **kwargs)
