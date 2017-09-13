# Json2Parquet [![Build Status](https://travis-ci.org/andrewgross/json2parquet.svg?branch=master)](https://travis-ci.org/andrewgross/json2parquet)

This library wraps `pyarrow` to provide some tools to easily convert JSON data into Parquet format.  It is mostly in Python.  It iterates over files.  It copies the data several times in memory.  It is not meant to be the fastest thing available.  However, it is convenient for smaller data sets, or people who don't have a huge issue with speed.

### Usage

Here's how to load a random JSON dataset.

```python
from json2parquet import convert_json

convert_json(input_filename, output_filename)
```
