<img align="centre" alt="overlapping arrows" height="92" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/mabel.svg" />

**mabel** is a fully-portable Data Engineering platform designed to run on low-spec compute nodes.

There is no server component, **mabel** just runs when you need it, where you want it.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mabel-dev/mabel/blob/master/LICENSE)
[![Status](https://img.shields.io/badge/status-beta-yellowgreen)](https://github.com/mabel-dev/mabel)
[![Regression Suite](https://github.com/mabel-dev/mabel/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/mabel-dev/mabel/actions/workflows/regression_suite.yaml)
[![codecov](https://codecov.io/gh/mabel-dev/mabel/branch/main/graph/badge.svg?token=CYD6E4PPKR)](https://codecov.io/gh/mabl-dev/mabel)
[![Static Analysis](https://github.com/mabel-dev/mabel/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/mabel-dev/mabel/actions/workflows/static_analysis.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/mabel.svg)](https://pypi.org/project/mabel/)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![mabel](https://snyk.io/advisor/python/mabel/badge.svg)](https://snyk.io/advisor/python/mabel)
[![Downloads](https://pepy.tech/badge/mabel)](https://pepy.tech/project/mabel)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


**Documentation** [GitHub Wiki](https://github.com/mabel-dev/mabel/wiki)  
**Bug Reports** [GitHub Issues](https://github.com/mabel-dev/mabel/issues/new/choose)  
**Feature Requests** [GitHub Issues](https://github.com/mabel-dev/mabel/issues/new/choose)  
**Source Code**  [GitHub](https://github.com/mabel-dev/mabel)  
**Discussions** [GitHub Discussions](https://github.com/mabel-dev/mabel/discussions)

## Focus on What Matters

We've built **mabel** to enable Data Analysts to write complex data engineering tasks
quickly and easily, so they could get on with doing what they do best.

~~~python
from mabel import operator
from mabel.operators import EndOperator

@operator
def say_hello(name):
    print(F"Hello, {name}!")

flow = say_hello > EndOperator()
with flow as runner:
    runner("world")  # Hello, world!
~~~

## Key Features

-  Programatically define data pipelines
-  Treats datasets as immutable
-  On-the-fly compression
-  Automatic version tracking of processing operations
-  Trace messages through the pipeline (random sampling)
-  Automatic retry of failed operations
-  Low-memory requirements, even with terabytes of data
-  Indexing and partitioning of data for fast reads (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />beta) 
-  Cursors for tracking reading position (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />beta) 
-  SQL Query support (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />alpha)
-  Schema and [data_expectations](https://github.com/joocer/data_expectations) validation

Note:
- **<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />alpha** features are subject to change and are not recommended for production systems  
- **<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />beta** features may change to resolve issues during testing

## Installation

From PyPI (recommended)
~~~
pip install --upgrade mabel
~~~
From GitHub
~~~
pip install --upgrade git+https://github.com/mabel-dev/mabel
~~~

## Guides

[How to Write a Flow](https://github.com/mabel-dev/mabel/wiki/how_to_write_a_flow)  
[How to Read Data](https://github.com/mabel-dev/mabel/wiki/how_to_read_a_dataset)

## Dependencies

-  **[dateutil](https://dateutil.readthedocs.io/en/stable/)** is used to convert dates received as strings
-  **[mmh3](https://github.com/hajimes/mmh3)** is used for non-cryptographic hashing
-  **[pydantic](https://pydantic-docs.helpmanual.io/)** is used to define internal data models  
-  **[UltraJSON](https://github.com/ultrajson/ultrajson)** (AKA `ujson`) is used where `orjson` is not available. (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />1)
-  **[zstandard](https://github.com/indygreg/python-zstandard)** is used for real-time compression


There are a number of optional dependencies which are usually only required for
specific features and functionality. These are listed in the
[requirements.txt](https://github.com/mabel-dev/mabel/blob/main/tests/requirements.txt)
file in the _tests_ folder which is used for testing. The exception is `orjson` which
is the preferred JSON library but not available on all platforms.

## Integrations

mabel comes with adapters for the following services:

| | Service | Support
|-- |-- |-- 
| <img align="centre" alt="GCP Storage" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/gcs-logo.png" /> | Google Cloud Storage |  Read/Write
| <img align="centre" alt="MinIo" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/minio-logo.png" /> | MinIO | Read/Write
| <img align="centre" alt="AWS S3" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/s3-logo.png" /> | S3 | Read/Write

MongoDB and MQTT Readers are included in the base library but are not supported.

## Deployment and Execution

mabel supports running on a range of platforms:

| | Platform |
|-- |-- |
| <img align="centre" alt="Docker" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/docker-logo.png" /> | Docker
| <img align="centre" alt="Kubernetes" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/kubernetes-logo.svg" /> | Kubernetes
| <img align="centre" alt="Raspberry Pi" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/raspberry-pi-logo.svg" /> | Raspberry Pi (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg">1)
| <img align="centre" alt="Windows" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/windows-logo.png" /> | Windows (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />2)
| <img align="centre" alt="Linux" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/linux-logo.jpg" /> | Linux (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />3)

MacOS also supported.

Adapters for other data services can be written. 

<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />1 - Raspbian fully functional with `ujson`.  
<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />2 - Multi-Processing not available on Windows. Alternate indexing libraries may be used on Windows.  
<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />3 - Tested on Debian and Ubuntu.

## How Can I Contribute?

All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.

If you have a suggestion for an improvement or a bug, 
[raise a ticket](https://github.com/mabel-dev/mabel/issues/new/choose) or start a
[discussion](https://github.com/mabel-dev/mabel/discussions).

Want to help build mabel? See the [contribution guidance](https://github.com/mabel-dev/mabel/blob/main/.github/CONTRIBUTING.md).

## License

[Apache 2.0](LICENSE)