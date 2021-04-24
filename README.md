<img align="centre" alt="overlapping arrows" height="92" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mabel.svg" />

**mabel** is a platform for authoring data processing systems.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/mabel/blob/master/LICENSE)
[![Regression Suite](https://github.com/joocer/mabel/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/regression_suite.yaml)
[![Static Analysis](https://github.com/joocer/mabel/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/static_analysis.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/mabel.svg)](https://pypi.org/project/mabel/)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![mabel](https://snyk.io/advisor/python/mabel/badge.svg)](https://snyk.io/advisor/python/mabel)
[![Downloads](https://img.shields.io/pypi/dm/mabel.svg)](https://pypi.org/project/mabel/)

## What are its Key Features

-  Programatically define data pipelines
-  Treats datasets as immutable
-  On-the-fly compression
-  Automatic version tracking of processing operations
-  Trace messages through the pipeline (random sampling)
-  Automatic retry of failed operations
-  Low-memory requirements, even with Tb of data

## Where Can I Find Documentation

See the [wiki](https://github.com/joocer/mabel/wiki)

## How Do I Get It?

From PyPI (recommended)
~~~
pip install --upgrade mabel
~~~
From GitHub
~~~
pip install --upgrade git+https://github.com/joocer/mabel
~~~

## What Dependencies does mabel Have?

-  **[UltraJSON](https://github.com/ultrajson/ultrajson)** (AKA `ujson`) is used where `orjson` is not available. `orjson` is the preferred JSON library but is not available on all platforms and environments so `ujson` is a dependency to ensure a performant JSON library with broad support is available.  
-  **[dateutil](https://dateutil.readthedocs.io/en/stable/)** is used to convert dates received as strings
-  **[zstandard](https://github.com/indygreg/python-zstandard)** is used for real-time compression
-  **[mmh3](https://github.com/hajimes/mmh3)** is used for non-cryptographic hashing
-  **[bitarray](https://github.com/ilanschnell/bitarray)** is used for compacting data

There are a number of optional dependencies which are usually only required for specific features and functionality. These are listed in the [requirements-test.txt](https://github.com/joocer/mabel/blob/main/requirements-test.txt) file which is used for testing. The key exception is `orjson` which is the preferred JSON library but not available on all platforms.

## Can I Contribute?

Want to help build mabel? See the [contribution guidance](https://github.com/joocer/mabel/blob/main/CONTRIBUTING.md)

## What Platforms Does mabel Support?

mabel comes with adapters for the following services, or is tested to run on the following platforms:

| | Service | Support
|-- |-- |-- 
| <img align="centre" alt="GCP Storage" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/gcs-logo.png" /> | Google Cloud Storage |  Read/Write
| <img align="centre" alt="MinIo" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/minio-logo.png" /> | MinIO | Read/Write
| <img align="centre" alt="AWS S3" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/s3-logo.png" /> | S3 | Read/Write
| <img align="centre" alt="MongoDB" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mongodb-logo.png" /> | MongoDB | Read Only
| <img align="centre" alt="MQTT" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mqtt-logo.png" /> | MQTT | Read Only
| <img align="centre" alt="Docker" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/docker-logo.png" /> | Docker | Hosting
| <img align="centre" alt="Kubernetes" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/kubernetes-logo.svg" /> | Kubernetes | Hosting
| <img align="centre" alt="Raspberry Pi" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/raspberry-pi-logo.svg" /> | Raspberry Pi | Hosting (<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1)

Linux, MacOS and Windows (<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />2) also supported.

Adapters for other data services can be written. 

<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1 - Raspbian fully functional with `ujson`  
<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />2 - Multi-Processing not available on Windows

## License
[Apache 2.0](LICENSE)