<img align="centre" alt="overlapping arrows" height="92" src="icons/mabel.svg" />

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

<img align="centre" alt="Google Cloud" height="64" src="icons/google-cloud-logo.png" />
<img align="centre" alt="MinIo" height="64" src="icons/minio-logo.png" />
<img align="centre" alt="MongoDB" height="64" src="icons/mongodb-logo.png" /> (partial)
<img align="centre" alt="MQTT" height="64" src="icons/mqtt-logo.png" /> (partial)
<img align="centre" alt="Raspberry Pi" height="64" src="icons/raspberry-pi-logo.png" />

## License
[Apache 2.0](LICENSE)