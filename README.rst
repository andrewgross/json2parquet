Json2Parquet |Build Status|
===========================

This library wraps ``pyarrow`` to provide some tools to easily convert
JSON data into Parquet format. It is mostly in Python. It iterates over
files. It copies the data several times in memory. It is not meant to be
the fastest thing available. However, it is convenient for smaller data
sets, or people who don't have a huge issue with speed.

Installation
~~~~~~~~~~~~

.. code:: bash

    pip install json2parquet

Usage
~~~~~

Here's how to load a random JSON dataset.

.. code:: python

    from json2parquet import convert_json

    # Infer Schema (requires reading dataset for column names)
    convert_json(input_filename, output_filename)

    # Given columns
    convert_json(input_filename, output_filename, ["my_column", "my_int"])

    # Given columns and custom field names
    field_aliases = {'my_column': 'my_updated_column_name', "my_int": "my_integer"}
    convert_json(input_filename, output_filename, ["my_column", "my_int"], field_aliases=field_aliases)


    # Given PyArrow schema
    import pyarrow as pa
    schema = pa.schema([
        pa.field('my_column', pa.string),
        pa.field('my_int', pa.int64),
    ])
    convert_json(input_filename, output_filename, schema)


You can also work with Python data structures directly


.. code:: python

    from json2parquet import load_json, ingest_data, write_parquet, write_parquet_dataset

    # Loading JSON to a PyArrow RecordBatch (schema is optional as above)
    load_json(input_filename, schema)

    # Working with a list of dictionaries
    ingest_data(input_data, schema)

    # Working with a list of dictionaries and custom field names
    field_aliases = {'my_column': 'my_updated_column_name', "my_int": "my_integer"}
    ingest_data(input_data, schema, field_aliases)

    # Writing Parquet Files from PyArrow Record Batches
    write_parquet(data, destination)

    # You can also pass any keyword arguments that PyArrow accepts
    write_parquet(data, destination, compression='snappy')

    # You can also write partitioned date
    write_parquet_dataset(data, destination_dir, partition_cols=["foo", "bar", "baz"])


If you know your schema, you can specify custom datetime formats (only one for now).  This formatting will be ignored if you don't pass a PyArrow schema.

.. code:: python

    from json2parquet import convert_json

    # Given PyArrow schema
    import pyarrow as pa
    schema = pa.schema([
        pa.field('my_column', pa.string),
        pa.field('my_int', pa.int64),
    ])
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    convert_json(input_filename, output_filename, schema, date_format=date_format)


Although ``json2parquet`` can infer schemas, it has helpers to pull in external ones as well

.. code:: python

    from json2parquet import load_json
    from json2parquet.helpers import get_schema_from_redshift

    # Fetch the schema from Redshift (requires psycopg2)
    schema = get_schema_from_redshift(redshift_schema, redshift_table, redshift_uri)

    # Load JSON with the Redshift schema
    load_json(input_filename, schema)


Operational Notes
~~~~~~~~~~~~~~~~~

If you are using this library to convert JSON data to be read by ``Spark``, ``Athena``, ``Spectrum`` or ``Presto`` make sure you use ``use_deprecated_int96_timestamps`` when writing your Parquet files, otherwise you will see some really screwy dates.


Contributing
~~~~~~~~~~~~


Code Changes
------------

- Clone a fork of the library
- Run ``make setup``
- Run ``make test``
- Apply your changes (don't bump version)
- Add tests if needed
- Run ``make test`` to ensure nothing broke
- Submit PR

Documentation Changes
---------------------

It is always a struggle to keep documentation correct and up to date.  Any fixes are welcome.  If you don't want to clone the repo to work locally, please feel free to edit using Github and to submit Pull Requests via Github's built in features.


.. |Build Status| image:: https://travis-ci.org/andrewgross/json2parquet.svg?branch=master
   :target: https://travis-ci.org/andrewgross/json2parquet
