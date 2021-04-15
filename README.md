**MABEL IS IN ALPHA - USAGE IS STILL STABLIZING**

<img align="centre" alt="overlapping arrows" height="92" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mabel.svg" />

**mabel** is a platform for authoring data processing systems.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/mabel/blob/master/LICENSE)
[![regression_suite](https://github.com/joocer/mabel/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/regression_suite.yaml)
[![bandit](https://github.com/joocer/mabel/actions/workflows/bandit.yaml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/bandit.yaml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/mabel.svg)](https://pypi.org/project/mabel/)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_mabel&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_mabel)

## Features

-  Programatically define data pipelines
-  Immutable datasets
-  On-the-fly compression
-  Automatic version tracking of processing operations
-  Trace messages through the pipeline (random sampling)
-  Automatic retry of operations

## Documentation

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

## Dependencies

-  **[UltraJSON](https://github.com/ultrajson/ultrajson)** (AKA `ujson`) is used where `orjson` is not available. `orjson` is the preferred JSON library but is not available on all platforms and environments so `ujson` is a dependency to ensure a performant JSON library with broad support is available.  
-  **[dateutil](https://dateutil.readthedocs.io/en/stable/)**
-  **[zstandard](https://github.com/indygreg/python-zstandard)**

There are a number of optional dependencies which are usually only required for specific features and functionality. These are listed in the [requirements-optional.txt](requirements-optional.txt) file which is used for testing. The key exception is `orjson` which is the preferred JSON library but not available on all platforms.

## Contributing

Want to help build mabel? See the [contribution guidance](CONTRIBUTING.md)

## What Platforms Does It Support?

mabel comes with adapters for the following services, or is tested to run on the following platforms:

| | Service | Support
|-- |-- |-- 
| <img align="centre" alt="Google Cloud Storage" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/gcs-logo.png" /> | Google Cloud Storage |  Read/Write
| <img align="centre" alt="MinIo" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/minio-logo.png" /> | MinIO | Read/Write
| <img align="centre" alt="MongoDB" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mongodb-logo.png" /> | MongoDB | Read
| <img align="centre" alt="MQTT" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/mqtt-logo.png" /> | MQTT | Read
| <img align="centre" alt="Docker" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/docker-logo.png" /> | Docker | Hosting
| <img align="centre" alt="Kubernetes" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/kubernetes-logo.svg" /> | Kubernetes | Hosting
| <img align="centre" alt="Raspberry Pi" height="48" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/raspberry-pi-logo.svg" /> | Raspberry Pi | Hosting (<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1)

Linux, MacOS and Windows (<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />2) also supported.

Adapters for other data services can be written. 

<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />1 - Raspbian fully functional with alternate JSON libraries  
<img align="centre" alt="Notice" height="16" src="https://raw.githubusercontent.com/joocer/mabel/main/icons/note.svg" />2 - Multi-Processing not available on Windows

## License
[Apache 2.0](LICENSE)