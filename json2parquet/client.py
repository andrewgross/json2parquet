# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

import pyarrow as pa
import pyarrow.parquet as pq


def ingest_data(data, schema=None):
    """
    Takes an array of dictionary objects, and a pyarrow schema with column names and types.
    Outputs a pyarrow Batch of the data
    """
    if isinstance(schema, list):
        return _convert_data_with_column_names(data, schema)
    elif isinstance(schema, pa.Schema):
        return _convert_data_with_schema(data, schema)
    else:
        return _convert_data_without_schema(data)

def _convert_data_without_schema(data):
    pass


def _convert_data_with_column_names(data, schema):
    pass


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
    Takes a PyArrow record batch and writes it as a parquet file to the gives destination
    """
    try:
        table = pa.Table.from_batches(data)
    except TypeError:
        table = pa.Table.from_batches([data])
    pq.write_table(table, destination, **kwargs)


def convert_json(input, output, schema, **kwargs):
    data = load_json(input, schema)
    write_parquet(data, output, **kwargs)

