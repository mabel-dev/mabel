# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Helper routines for handling types between different dialects.
"""
from enum import Enum
from mabel.exceptions import UnsupportedTypeError
import datetime


def coerce_types(value):
    """
    Relations only support a subset of types, if we know how to translate a type
    into a supported type, do it.
    """
    t = type(value)
    if t in (int, float, tuple, bool, str, datetime.datetime, dict):
        return value
    if t in (list, set):
        return tuple(value)
    if t in (datetime.date,):
        return datetime.datetime(t.year, t.month, t.day)
    if value is None:
        return value
    raise UnsupportedTypeError(
        f"Attributes of type `{t}` are not supported - the value was `{value}`"
    )


def coerce_values(value, value_type):
    #
    pass


class MABEL_TYPES(str, Enum):
    BOOLEAN = "BOOLEAN"
    NUMERIC = "NUMERIC"
    LIST = "LIST"
    VARCHAR = "VARCHAR"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"
    OTHER = "OTHER"


MABEL_TYPE_NAMES = {
    MABEL_TYPES.BOOLEAN: "BOOLEAN",
    MABEL_TYPES.NUMERIC: "NUMERIC",
    MABEL_TYPES.LIST: "LIST",
    MABEL_TYPES.VARCHAR: "VARCHAR",
    MABEL_TYPES.STRUCT: "STRUCT",
    MABEL_TYPES.TIMESTAMP: "TIMESTAMP",
    MABEL_TYPES.OTHER: "OTHER",
}


PYTHON_TYPES = {
    "bool": MABEL_TYPES.BOOLEAN,
    "datetime": MABEL_TYPES.TIMESTAMP,
    "date": MABEL_TYPES.TIMESTAMP,
    "dict": MABEL_TYPES.STRUCT,
    "int": MABEL_TYPES.NUMERIC,
    "float": MABEL_TYPES.NUMERIC,
    "Decimal": MABEL_TYPES.NUMERIC,
    "str": MABEL_TYPES.VARCHAR,
    "tuple": MABEL_TYPES.LIST,
    "list": MABEL_TYPES.LIST,
    "set": MABEL_TYPES.LIST
}

PARQUET_TYPES = {
    "bool": MABEL_TYPES.BOOLEAN,
    "timestamp[ms]": MABEL_TYPES.TIMESTAMP,
    "dict": MABEL_TYPES.STRUCT,
    "int64": MABEL_TYPES.NUMERIC,
    "double": MABEL_TYPES.NUMERIC,
    "string": MABEL_TYPES.VARCHAR,
    "tuple": MABEL_TYPES.LIST,
}



def get_coerced_type(python_type):
    if python_type in PYTHON_TYPES:
        return PYTHON_TYPES[python_type].name
    return f"unknown ({python_type})"
