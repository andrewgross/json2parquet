Changelog
---------

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
