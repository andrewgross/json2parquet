#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re

from setuptools import setup, find_packages

with open('json2parquet/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.rst') as changelog_file:
    changelog = changelog_file.read()


setup(
    name='json2parquet',
    version=version,
    description='A simple Parquet converter for JSON/python data',
    long_description=readme + '\n\n' + changelog,
    author='Andrew Gross',
    author_email='andrew.w.gross@gmail.com',
    url='https://github.com/andrewgross/json2parquet',
    install_requires=[
        'pyarrow==0.8.0',
        'pandas==0.20.3'
    ],
    packages=[n for n in find_packages() if not n.startswith('tests')],
    include_package_data=True,
)
