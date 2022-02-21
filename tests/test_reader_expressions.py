"""
We're testing the Expression module, but for simplicity we're testing it via
the .query() method on the Relation, this makes handling the data we're querying
easier to handle.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data.internals.relation import Relation
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

    DATA = Relation(TEST_DATA)

    assert DATA.apply_selection(("name", "=", "James Potter")).count() == 2
    assert DATA.apply_selection(("age", "=", 10)).count() == 1
    assert DATA.apply_selection(("alive", "=", True)).count() == 3
    assert DATA.apply_selection(("affiliations", "is", None)).count() == 4
    assert DATA.apply_selection(("name", "like", "%Potter%")).count() == 4


def test_simple_not_expressions():

    DATA = Relation(TEST_DATA)

    assert DATA.apply_selection(("name", "<>", "James Potter")).count() == 5
    assert DATA.apply_selection(("age", "<>", 10)).count() == 6
    assert DATA.apply_selection(("alive", "=", False)).count() == 4
    assert DATA.apply_selection(("affiliations", "!=", None)).count() == 3
    assert DATA.apply_selection(("name", "not like", "%Potter%")).count() == 3


def test_simple_compound_expressions():
    DATA = Relation(TEST_DATA)

    assert DATA.apply_selection([("name", "like", "%Potter"), ("alive", "=", True)]).count() == 2
    assert DATA.apply_selection([[("name", "like", "%Potter")], [("alive", "=", True)]]).count() == 5


if __name__ == "__main__":  # pragma: no cover

    test_simple_equals_expressions()
    test_simple_not_expressions()
    test_simple_compound_expressions()

    print("okay")
