# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import filecmp
import os
import tempfile

import pandas as pd
import pyarrow as pa

from json2parquet import client


def test_ingest():
    """
    Test ingesting data with a given schema
    """
    schema = pa.schema([
        pa.field("foo", pa.int64()),
        pa.field("bar", pa.int64())
    ])

    data = [{"foo": 1, "bar": 2}, {"foo": 10, "bar": 20}]

    converted_data = client.ingest_data(data, schema)
    assert converted_data.to_pydict() == {'foo': [1, 10], 'bar': [2, 20]}


def test_ingest_with_datetime():
    """
    Test ingesting datetime data with a given schema
    """
    schema = pa.schema([
        pa.field("foo", pa.int64()),
        pa.field("bar", pa.int64()),
        pa.field("baz", pa.timestamp("ns"))
    ])

    data = [{"foo": 1, "bar": 2, "baz": "2018-01-01 01:02:03"}, {"foo": 10, "bar": 20, "baz": "2018-01-02 01:02:03"}]

    converted_data = client.ingest_data(data, schema)
    timestamp_values = [pd.to_datetime("2018-01-01 01:02:03"), pd.to_datetime("2018-01-02 01:02:03")]
    assert converted_data.to_pydict() == {'foo': [1, 10], 'bar': [2, 20], 'baz': timestamp_values}


def test_ingest_with_datetime_formatted():
    """
    Test ingesting datetime data with a given schema and custom date format
    """
    schema = pa.schema([
        pa.field("foo", pa.int64()),
        pa.field("bar", pa.int64()),
        pa.field("baz", pa.timestamp("ns"))
    ])

    data = [{"foo": 1, "bar": 2, "baz": "2018/01/01 01:02:03"}, {"foo": 10, "bar": 20, "baz": "2018/01/02 01:02:03"}]

    converted_data = client.ingest_data(data, schema, date_format="%Y/%m/%d %H:%M:%S")
    timestamp_values = [pd.to_datetime("2018-01-01 01:02:03"), pd.to_datetime("2018-01-02 01:02:03")]
    assert converted_data.to_pydict() == {'foo': [1, 10], 'bar': [2, 20], 'baz': timestamp_values}


def test_ingest_with_column_names():
    """
    Test ingesting data with given column names
    """
    schema = ["foo", "bar"]

    data = [{"foo": 1, "bar": 2}, {"foo": 10, "bar": 20}]

    converted_data = client.ingest_data(data, schema)
    assert converted_data.to_pydict() == {'foo': [1, 10], 'bar': [2, 20]}


def test_ingest_with_no_schema():
    """
    Test ingesting data with no schema
    """
    data = [{"foo": 1, "bar": 2}, {"foo": 10, "bar": 20}]

    converted_data = client.ingest_data(data)
    assert converted_data.to_pydict() == {'foo': [1, 10], 'bar': [2, 20]}


def test_ingest_with_no_schema_and_uneven_column_names():
    """
    Test ingesting data with no schema and incomplete JSON records
    """
    data = [{"foo": 1, "bar": 2}, {"foo": 10, "bar": 20}, {"foo": 100, "bar": 200, "baz": 300}]

    converted_data = client.ingest_data(data)
    assert converted_data.to_pydict() == {'foo': [1, 10, 100], 'bar': [2, 20, 200], 'baz': [None, None, 300]}


def test_load_json():
    """
    Test loading JSON from a file
    """
    schema = pa.schema([
        pa.field("foo", pa.int32()),
        pa.field("bar", pa.int64())
    ])

    path = "{}/tests/fixtures/simple_json.txt".format(os.getcwd())

    converted_data = client.load_json(path, schema)
    assert converted_data.to_pydict() == {'foo': [1, 10], 'bar': [2, 20]}


def test_convert_json():
    """
    Test converting a JSON file to Parquet
    """
    schema = pa.schema([
        pa.field("foo", pa.int32()),
        pa.field("bar", pa.int64())
    ])

    input_path = "{}/tests/fixtures/simple_json.txt".format(os.getcwd())
    expected_file = "{}/tests/fixtures/simple.parquet".format(os.getcwd())
    with tempfile.NamedTemporaryFile() as f:
        output_path = f.name
        client.convert_json(input_path, output_path, schema)
        assert filecmp.cmp(expected_file, output_path)
