# -*- coding: utf-8 -*-
from __future__ import unicode_literals

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
