<img align="centre" alt="VAP" height="64" src="mabel-sm.png" />

**mabel** is a platform to help with authoring data processing systems.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/mabel/blob/master/LICENSE)
[![integration_tests](https://github.com/joocer/mabel/actions/workflows/integration_tests.yaml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/integration_tests.yaml)
[![bandit](https://github.com/joocer/mabel/actions/workflows/bandit.yaml/badge.svg)](https://github.com/joocer/mabel/actions/workflows/bandit.yaml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/mabel.svg)](https://pypi.org/project/mabel/)

## What Is In It?

mabel.flows -   
mabel.operators - logging routines      
mabel.adapters -   
mabel.data - read data from various sources      
mabel.data.formats - helpers for handling data   
mabel.data.validator - schema conformity testing   



## How Do I Get It?
~~~
pip install --upgrade git+https://github.com/joocer/mabel
~~~


## Features

- Programatically define data pipelines
- Immutable datasets
- On-the-fly compression

## Concepts

### Flows

- **Flow** -
- **Operator** -
- **Run** - 

### Data

- **Dataset** -  
- **Partition** - The files in a dataset are split into chunks of 32Mb and in date formatted folders. 
- **Frame** - Batch data is written into a frame for each execution of the batch. Frames exist as folders with a prefix 'as_at_' indicating the time the batch was run. 

## Dependencies

- **[UltraJSON](https://github.com/ultrajson/ultrajson)** (AKA `ujson`) is used where `orjson` is not available. `orjson` is the preferred JSON library but is not available on all platforms and environments so `ujson` is a dependency to ensure a performant JSON library with broad support is available
- DateUtil
- zstandard 

There are a number of optional dependencies which are required for specific features and functionality. These are listed in the [requirements-optional.txt](requirements-optional.txt) file.
