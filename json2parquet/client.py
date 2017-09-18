# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def ingest_data(data, schema=None):
    """
    data: Array of Dictionary objects
    schema: PyArrow schema object or list of column names

    return: a PyArrow Batch
    """
    if isinstance(schema, list):
        return _convert_data_with_column_names(data, schema)
    elif isinstance(schema, pa.Schema):
        return _convert_data_with_schema(data, schema)
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


def _convert_data_with_schema(data, schema):
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
            _converted_datetimes = []
            for item in _col:
                _converted_datetimes.append(pd.to_datetime(item))
            _col = _converted_datetimes
        array_data.append(pa.array(_col, type=column.type))
    return pa.RecordBatch.from_arrays(array_data, schema.names)


def load_json(filename, schema):
    """
    Simple but inefficient way to load data from a newline delineated json file
    """
    json_data = []
    with open(filename, "r") as f:
        for line in f.readlines():
            if line:
                json_data.append(json.loads(line))
    return ingest_data(json_data, schema)


def write_parquet(data, destination, **kwargs):
    """
    Takes a PyArrow record batch and writes it as parquet
    """
    try:
        table = pa.Table.from_batches(data)
    except TypeError:
        table = pa.Table.from_batches([data])
    pq.write_table(table, destination, **kwargs)


def convert_json(input, output, schema, **kwargs):
    data = load_json(input, schema)
    write_parquet(data, output, **kwargs)
