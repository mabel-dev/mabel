# DEVELOPMENT MABEL

<img align="centre" alt="overlapping arrows" height="92" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/mabel.svg" />

## mabel is a Data Engineering platform designed to run in serverless environments.

**mabel** has no server component, **mabel** just runs when you need it making it ideal
for deployments to platforms like Kubernetes, Cloud Run and Knative.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mabel-dev/mabel/blob/master/LICENSE)
[![Status](https://img.shields.io/badge/status-beta-yellowgreen)](https://github.com/mabel-dev/mabel)
[![Regression Suite](https://github.com/mabel-dev/mabel/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/mabel-dev/mabel/actions/workflows/regression_suite.yaml)
[![codecov](https://codecov.io/gh/mabel-dev/mabel/branch/main/graph/badge.svg?token=CYD6E4PPKR)](https://codecov.io/gh/mabl-dev/mabel)
[![Static Analysis](https://github.com/mabel-dev/mabel/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/mabel-dev/mabel/actions/workflows/static_analysis.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/mabel.svg)](https://pypi.org/project/mabel/)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![mabel](https://snyk.io/advisor/python/mabel/badge.svg)](https://snyk.io/advisor/python/mabel)
[![deepcode](https://www.deepcode.ai/api/gh/badge?key=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwbGF0Zm9ybTEiOiJnaCIsIm93bmVyMSI6Impvb2NlciIsInJlcG8xIjoibWFiZWwiLCJpbmNsdWRlTGludCI6ZmFsc2UsImF1dGhvcklkIjoyNTcxMiwiaWF0IjoxNjE5MjUyNzkxfQ.UtjaLJQjVxoQjesfMXuQ-tnbvJBUEzMUSJAC_neucek)](https://www.deepcode.ai/app/gh/mabel-dev/mabel/_/dashboard?utm_content=gh%2Fmabel-dev%2Fmabel)
[![Downloads](https://pepy.tech/badge/mabel)](https://pepy.tech/project/mabel)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


- **Documentation** [GitHub Wiki](https://github.com/mabel-dev/mabel/wiki)  
- **Bug Reports** [GitHub Issues](https://github.com/mabel-dev/mabel/issues/new/choose)  
- **Feature Requests** [GitHub Issues](https://github.com/mabel-dev/mabel/issues/new/choose)  
- **Source Code**  [GitHub](https://github.com/mabel-dev/mabel)  
- **Discussions** [GitHub Discussions](https://github.com/mabel-dev/mabel/discussions)

## Focus on What Matters

We've built **mabel** to enable Data Analysts to write complex data engineering tasks
quickly and easily, so they could get on with doing what they do best.

~~~python
from mabel import Reader

data = Reader(dataset="test_data")
print(data.count())
~~~

## Key Features

-  On-the-fly compression
-  Low-memory requirements, even with terabytes of data
-  Indexing and partitioning of data for fast reads 
-  Cursors for tracking reading position between processes 
-  Partial SQL DQL support 
-  Schema and [data_expectations](https://github.com/joocer/data_expectations) validation

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

[How to Read Data](https://github.com/mabel-dev/mabel/wiki/how_to_read_a_dataset)

## Dependencies

-  **[orjson](https://github.com/ijl/orjson)** is used for JSON handling
-  **[dateutil](https://dateutil.readthedocs.io/en/stable/)** is used to convert dates received as strings
-  **[cityhash](https://github.com/google/cityhash)** is used for non-cryptographic hashing
-  **[pydantic](https://pydantic-docs.helpmanual.io/)** is used to define internal data models  
-  **[zstandard](https://github.com/indygreg/python-zstandard)** is used for real-time compression


There are a number of optional dependencies which are usually only required for
specific features and functionality. These are listed in the
[requirements.txt](https://github.com/mabel-dev/mabel/blob/main/tests/requirements.txt)
file in the _tests_ folder.

## Integrations

mabel comes with adapters for the following services:

|   | Service |
|-- |-- |
| <img align="centre" alt="GCP Storage" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/gcs-logo.png" /> | Google Cloud Storage |
| <img align="centre" alt="MinIo" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/minio-logo.png" /> | MinIO |
| <img align="centre" alt="AWS S3" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/s3-logo.png" /> | S3 | 
| <img align="centre" alt="Azure" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/azure.svg" /> | Azure |

## Deployment and Execution

mabel supports running on a range of platforms:

|   | Platform |
|-- |-- |
| <img align="centre" alt="Docker" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/docker-logo.png" /> | Docker
| <img align="centre" alt="Kubernetes" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/kubernetes-logo.svg" /> | Kubernetes
| <img align="centre" alt="Windows" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/windows-logo.png" /> | Windows (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />1)
| <img align="centre" alt="Linux" height="48" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/linux-logo.jpg" /> | Linux (<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />2)

Adapters for other data services can be written. 

<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />1 - Multi-Processing not available on Windows.
<img align="centre" alt="Notice" height="12" src="https://raw.githubusercontent.com/mabel-dev/mabel/main/icons/note.svg" />2 - Tested on Debian (WSL) and Ubuntu.

## How Can I Contribute?

All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.

If you have a suggestion for an improvement or a bug, 
[raise a ticket](https://github.com/mabel-dev/mabel/issues/new/choose) or start a
[discussion](https://github.com/mabel-dev/mabel/discussions).

Want to help build mabel? See the [contribution guidance](https://github.com/mabel-dev/mabel/blob/main/.github/CONTRIBUTING.md).

## License

[Apache 2.0](LICENSE)