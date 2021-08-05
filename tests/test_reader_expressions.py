"""
We're testing the Expression module, but for simplicity we're testing it via
the .query() method on the DictSet, this makes handling the data we're querying
easier to handle.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.readers import STORAGE_CLASS
from mabel import DictSet
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

def test_simple_equals_expressions():

    DATA = DictSet(TEST_DATA, STORAGE_CLASS.MEMORY)

    assert DATA.query("name == 'James Potter'").count() == 2
    assert DATA.query("age == 10").count() == 1
    assert DATA.query("alive == true").count() == 3
    assert DATA.query("affiliations is none").count() == 4
    assert DATA.query("name like '%Potter%'").count() == 4


def test_simple_not_expressions():

    DATA = DictSet(TEST_DATA, STORAGE_CLASS.MEMORY)

    assert DATA.query("name <> 'James Potter'").count() == 5
    assert DATA.query("not age == 10").count() == 6
    assert DATA.query("alive == false").count() == 4
    assert DATA.query("affiliations is not none").count() == 3
    assert DATA.query("name not like '%Potter%'").count() == 3


def test_simple_compound_expressions():
    DATA = DictSet(TEST_DATA, STORAGE_CLASS.MEMORY)

    assert DATA.query("name like '%Potter' and alive == true").count() == 2
    assert DATA.query("name like '%Potter' or alive == true").count() == 5


if __name__ == "__main__":  # pragma: no cover
    test_simple_equals_expressions()
    test_simple_not_expressions()
    test_simple_compound_expressions()


    print("okay")
