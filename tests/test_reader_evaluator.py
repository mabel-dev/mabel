"""
We're testing the Expression module, but for simplicity we're testing it via
the .query() method on the DictSet, this makes handling the data we're querying
easier to handle.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from rich import traceback
from mabel.data.readers.internals.inline_evaluator import Evaluator

traceback.install()


TEST_DATA = {
    "name": "Sirius Black",
    "age": 40,
    "alive": False,
    "dob": "1970-01-02",
    "gender": "male",
    "affiliations": ["OotP"],
}


def test_simple_fields():
    pf = "name, age, alive, dob, gender"
    res = Evaluator(pf)(TEST_DATA)
    assert res == {
        "name": "Sirius Black",
        "age": 40,
        "alive": False,
        "dob": "1970-01-02",
        "gender": "male",
    }, res


def test_simple_functions():
    pf = "UPPER(name), YEAR(dob), LEFT(gender, 2), RIGHT(gender, 2), MID(name, 5, 3)"
    res = Evaluator(pf)(TEST_DATA)
    assert res == {
        "UPPER(name)": "SIRIUS BLACK",
        "YEAR(dob)": 1970,
        "LEFT(gender,2)": "ma",
        "RIGHT(gender,2)": "le",
        "MID(name,5,3)": "s B",
    }, res


def test_concat():
    pf = "CONCAT(name,' ',name,' table')"
    res = Evaluator(pf)(TEST_DATA)
    assert res == {"CONCAT(name,' ',name,' table')": "Sirius Black Sirius Black table"}, res


def test_as():
    pf = "UPPER(name) as upper_name"
    res = Evaluator(pf)(TEST_DATA)
    assert res == {"upper_name": "SIRIUS BLACK"}


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
