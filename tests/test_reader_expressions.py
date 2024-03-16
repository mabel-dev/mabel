"""
We're testing the Expression module, but for simplicity we're testing it via
the .query() method on the DictSet, this makes handling the data we're querying
easier to handle.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import STORAGE_CLASS
from mabel import DictSet
from mabel.data.internals.expression import Expression
from rich import traceback

traceback.install()

# fmt: off
TEST_DATA = [
    { "name": "Sirius Black", "age": 40, "alive": False, "dob": "1970-01-02", "gender": "male", "affiliations": ['OotP'] },
    { "name": "Harry Potter", "age": 11, "alive": True, "dob": "1999-07-30", "gender": "male", "affiliations": ['Dumbledores Army', 'Griffindor'] },
    { "name": "Hermione Grainger", "age": 10, "alive": True, "dob": "1999-12-14", "gender": "female", "affiliations": ['Dumbledores Army', 'Griffindor', 'MoM'] },
    { "name": "Fleur Isabelle Delacour", "age": 11, "alive": False, "dob": "1999-02-08", "gender": "female" },
    { "name": "James Potter", "age": 40, "alive": False, "dob": "1971-12-30", "gender": "male" },
    { "name": "James Potter", "age": 0, "alive": True, "dob": "2010-12-30", "gender": "male" },
    { "name": "Lily Potter", "age": 39, "alive": False, "dob": "1972-01-12", "gender": "female"}
]
# fmt: on


def test_expression_compilation():
    # fmt: off
    EXPRESSIONS = [
        {"expression": "YEAR(dob) == 1979", "dnf": ("YEAR(dob)", "==", 1979)},
        {"expression": "name == 'James Potter'","dnf": ("name", "==", '"James Potter"'),},
        {"expression": "not alive == True", "dnf": ("NOT", ("alive", "==", True))},
        {"expression": "age < 100 and alive == true","dnf": [("age", "<", 100), ("alive", "==", True)],},
        {"expression": "dob == '1979-09-19' or name like '%potter%'","dnf": [[("dob", "==", '"1979-09-19 00:00:00"')],[("name", "LIKE", '"%potter%"')],],},
        {"expression": "`name` == 'James Potter'", "dnf": ("`name`", "==", '"James Potter"')},
    ]
    # fmt: on

    for e in EXPRESSIONS:
        exp = Expression(e["expression"])
        dnf = exp.to_dnf()
        assert dnf == e["dnf"], dnf


def test_simple_equals_expressions():
    DATA = DictSet(TEST_DATA, storage_class=STORAGE_CLASS.MEMORY)

    assert DATA.filter("name == 'James Potter'").count() == 2
    assert DATA.filter("age == 10").count() == 1
    assert DATA.filter("alive == true").count() == 3
    assert DATA.filter("affiliations is none").count() == 4
    assert DATA.filter("name like '%Potter%'").count() == 4


def test_simple_not_expressions():
    DATA = DictSet(TEST_DATA, storage_class=STORAGE_CLASS.MEMORY)

    assert DATA.filter("name <> 'James Potter'").count() == 5
    assert DATA.filter("not age == 10").count() == 6
    assert DATA.filter("alive == false").count() == 4
    assert DATA.filter("affiliations is not none").count() == 3
    assert DATA.filter("name not like '%Potter%'").count() == 3


def test_simple_compound_expressions():
    DATA = DictSet(TEST_DATA, storage_class=STORAGE_CLASS.MEMORY)

    assert DATA.filter("name like '%Potter' and alive == true").count() == 2
    assert DATA.filter("name like '%Potter' or alive == true").count() == 5


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
