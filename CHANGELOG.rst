Changelog
---------

0.0.27
~~~~~~
- Update `pyarrow` to `0.0.12` to fix zlib issues and gain improved memory perf with lots of strings.  Thanks @nimish and @Madhu1512

0.0.26
~~~~~~
- Add `field_aliases` kwarg to loading methods to allow mapping a JSON column name to a different parquet column name.  Thanks to @sojovi for the idea.

0.0.25
~~~~~~
- Add `write_parquet_dataset` available as a default export, thanks to @gregburek

0.0.24
~~~~~~
- Bump pyarrow, lower lock on Numpy to `>=1.14`

0.0.23
~~~~~~
- Bump pyarrow, numpy and Pandas versions

0.0.22
~~~~~~
- Bump pyarrow and Pandas versions

0.0.21
~~~~~~
- Don't lock ciso8601 version.

0.0.20
~~~~~~
- Add support for DATE fields. h/t to Spectrify for the implementation

0.0.19
~~~~~~
- Properly handle boolean columns with `None`.

0.0.18
~~~~~~
- Allow `schema` to be an optional argument to `convert_json`

0.0.17
~~~~~~
- Bring `write_parquet_dataset` to a top level import

0.0.16
~~~~~~
- Properly convert Boolean fields passed as numbers to PyArrow booleans.

0.0.15
~~~~~~
- Add support for custom datetime formatting (thanks @Madhu1512)
- Add support for writing partitioned datasets (thanks @mthota15)

0.0.14
~~~~~~
- Stop silencing Redshift errors.

0.0.13
~~~~~~
- Fix decimal type for newer pyarrow versions

0.0.12
~~~~~~
- Allow casting of int64 -> int32

0.0.11
~~~~~~
- Bump PyArrow and allow int32 data

0.0.10
~~~~~~
- Allow passing partition columns when getting a Redshift schema, so they can be skipped

0.0.9
~~~~~~
- Fix conversion of timestamp columns again

0.0.8
~~~~~~
- Fix conversion of timestamp columns

0.0.7
~~~~~~
- Force converted Timestamps to max out at `pandas.Timestamp.max` if they exceed the resolution of `datetime[ns]`

0.0.6
~~~~~~
- Add automatic downcasting for Python ``float`` to ``float32`` via pandas when schema specifies ``pa.float32()``

0.0.5
~~~~~~
- Fix conversion of float types to be size specific

0.0.4
~~~~~~
- Fix ingestion of timestamp data with ns resolution

0.0.3
~~~~~~
- Add pandas dependency
- Add proper ingestion of timestamp data using Pandas ``to_datetime``

0.0.2
~~~~~~
- Fix formatting of README so it displays on PyPI

0.0.1
~~~~~~

- Initial release
- JSON/data writing support
- Redshift Schema reading support
