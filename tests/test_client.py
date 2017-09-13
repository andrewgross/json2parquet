# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import filecmp
import os
import tempfile

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


def test_load_json():
    """
    Test loading JSON from a file
    """
    schema = pa.schema([
        pa.field("foo", pa.int64()),
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
        pa.field("foo", pa.int64()),
        pa.field("bar", pa.int64())
    ])

    input_path = "{}/tests/fixtures/simple_json.txt".format(os.getcwd())
    expected_file = "{}/tests/fixtures/simple.parquet".format(os.getcwd())
    with tempfile.NamedTemporaryFile() as f:
        output_path = f.name
        client.convert_json(input_path, output_path, schema)
        assert filecmp.cmp(expected_file, output_path)
