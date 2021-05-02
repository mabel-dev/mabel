<img align="centre" alt="overlapping arrows" height="92" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mabel.svg" />

**mabel** is a platform for authoring data processing systems.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/mabel/blob/master/LICENSE)
[![Regression Suite](https://github.com/joocer/mabel/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/regression_suite.yaml)
[![codecov](https://codecov.io/gh/joocer/mabel/branch/main/graph/badge.svg?token=CYD6E4PPKR)](https://codecov.io/gh/joocer/mabel)
[![Static Analysis](https://github.com/joocer/mabel/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/static_analysis.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/mabel.svg)](https://pypi.org/project/mabel/)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![mabel](https://snyk.io/advisor/python/mabel/badge.svg)](https://snyk.io/advisor/python/mabel)
[![deepcode](https://www.deepcode.ai/api/gh/badge?key=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwbGF0Zm9ybTEiOiJnaCIsIm93bmVyMSI6Impvb2NlciIsInJlcG8xIjoibWFiZWwiLCJpbmNsdWRlTGludCI6ZmFsc2UsImF1dGhvcklkIjoyNTcxMiwiaWF0IjoxNjE5MjUyNzkxfQ.UtjaLJQjVxoQjesfMXuQ-tnbvJBUEzMUSJAC_neucek)](https://www.deepcode.ai/app/gh/joocer/mabel/_/dashboard?utm_content=gh%2Fjoocer%2Fmabel)
[![Downloads](https://pepy.tech/badge/mabel)](https://pepy.tech/project/mabel)


**Documentation** [GitHub Wiki](https://github.com/joocer/mabel/wiki)  
**Bug Reports** [GitHub Issues](https://github.com/joocer/mabel/issues/new/choose)  
**Feature Requests** [GitHub Issues](https://github.com/joocer/mabel/issues/new/choose)  
**Source Code**  [GitHub](https://github.com/joocer/mabel)  
**Discussions** [GitHub Discussions](https://github.com/joocer/mabel/discussions)


## Key Features

-  Programatically define data pipelines
-  Treats datasets as immutable
-  On-the-fly compression
-  Automatic version tracking of processing operations
-  Trace messages through the pipeline (random sampling)
-  Automatic retry of failed operations
-  Low-memory requirements, even with Tb of data

## Installation

From PyPI (recommended)
~~~
pip install --upgrade mabel
~~~
From GitHub
~~~
pip install --upgrade git+https://github.com/joocer/mabel
~~~

## Guides

[How to Write a Flow](https://github.com/joocer/mabel/wiki/how_to_write_a_flow)  
[How to Read Data](https://github.com/joocer/mabel/wiki/how_to_read_a_dataset)

## Dependencies

-  **[UltraJSON](https://github.com/ultrajson/ultrajson)** (AKA `ujson`) is used where `orjson` is not available. (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1)
-  **[dateutil](https://dateutil.readthedocs.io/en/stable/)** is used to convert dates received as strings
-  **[zstandard](https://github.com/indygreg/python-zstandard)** is used for real-time compression
-  **[mmh3](https://github.com/hajimes/mmh3)** is used for non-cryptographic hashing

There are a number of optional dependencies which are usually only required for specific features and functionality. These are listed in the [requirements-test.txt](https://github.com/joocer/mabel/blob/main/requirements-test.txt) file which is used for testing. The key exception is `orjson` which is the preferred JSON library but not available on all platforms.

## Can I Contribute?

All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.

Want to help build mabel? See the [contribution guidance](https://github.com/joocer/mabel/blob/main/.github/CONTRIBUTING.md)

## Platform Support?

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
| <img align="centre" alt="Raspberry Pi" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/raspberry-pi-logo.svg" /> | Raspberry Pi | Hosting (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1)

Linux, MacOS and Windows (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />2) also supported.

Adapters for other data services can be written. 

<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1 - Raspbian fully functional with `ujson`  
<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />2 - Multi-Processing not available on Windows. Alternate indexing libraries may be used on Windows.

## License

[Apache 2.0](LICENSE)