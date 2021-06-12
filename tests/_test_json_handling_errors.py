"""
This intentionally breaks the import of orjson even if it is installed on the
system. It does this by removing the path to orjson from the paths it looks at
for imports - this is where a lot of the imports, so before we remove the path
we have to import some that are not used by any of the active code.

This breaks pytest, so we've renamed the file but kept the test so it doesn't
automatically run.
"""
import os
import sys

import hashlib
import typing
import ujson
import datetime
import pathlib
import dataclasses
import traceback
import uuid
import json
import string
import logging
import pydantic
import dateutil
import __future__
import calendar
import six
import bisect
import tempfile
import mmh3
import queue
import multiprocessing
import rich
import platform
import pygments
import subprocess
import getpass
import configparser
import fractions
import colorama
import textwrap

base = sys.path[0]
sys.path.clear()
sys.path.insert(1, os.path.join(base, ".."))
sys.path.insert(1, os.path.join(base, "../.."))
from mabel.data.formats.json import parse, serialize
from rich import traceback

traceback.install()

will_normally_fail = {
    "string": "string",
    "number": 100,
    "date": datetime.date(2015, 6, 1),
    "datetime": datetime.datetime(1979, 9, 10, 23, 13),
    "list": ["item"],
    "nothing": None,
}


def test_json_serializing():

    failed = False

    try:
        b = serialize(will_normally_fail, as_bytes=True)
    except:
        failed = True

    assert not failed, "didn't process all types"
    assert isinstance(b, bytes), f"didn't return bytes - {type(b)}"
    print(b)


if __name__ == "__main__":  # pragma: no cover
    test_json_serializing()

    print("okay")
