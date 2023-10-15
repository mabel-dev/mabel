import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data.internals.dnf_filters import DnfFilters
from mabel.data.internals.dictset import STORAGE_CLASS
from mabel.data import Reader
from rich import traceback

traceback.install()

# fmt: off
TEST_DATA = [
    { "name": "Sirius Black", "age": 40, "dob": "1970-01-02", "gender": "male", "affiliations": ['OotP'] },
    { "name": "Harry Potter", "age": 11, "dob": "1999-07-30", "gender": "male", "affiliations": ['Dumbledores Army', 'Griffindor'] },
    { "name": "Hermione Grainger", "age": 10, "dob": "1999-12-14", "gender": "female", "affiliations": ['Dumbledores Army', 'Griffindor', 'MoM'] },
    { "name": "Fleur Isabelle Delacour", "age": 11, "dob": "1999-02-08", "gender": "female" },
    { "name": "James Potter", "age": 40, "dob": "1971-12-30", "gender": "male" },
    { "name": "James Potter", "age": 0, "dob": "2010-12-30", "gender": "male" }
]
# fmt: on


def test_reader_filters_no_filter():
    """ensure the reader filter is working as expected"""
    r = Reader(inner_reader=DiskReader, dataset="tests/data/tweets/", raw_path=True)
    for index, item in enumerate(r):
        pass
    assert index == 49, index


def test_reader_filters_single_filter():
    """ensure the reader filter is working as expected"""
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        raw_path=True,
        filters="username == 'NBCNews'",
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert r.count() == 44, r.count()


def test_reader_filters_multiple_filter():
    """ensure the reader filter is working as expected"""
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        raw_path=True,
        filters="username = 'NBCNews' and timestamp >= '2020-01-12T07:11:04'",
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert r.count() == 34, r


def test_filters():
    d = DnfFilters(filters=[("age", "==", 11), ("gender", "in", ("a", "b", "male"))])
    assert len(list(d.filter_dictset(TEST_DATA))) == 1


def test_empty_filters():
    d = DnfFilters(filters=None)
    assert len(list(d.filter_dictset(TEST_DATA))) == 6


def test_like_filters():
    d = DnfFilters(filters=[("dob", "like", "%-12-%")])
    assert len(list(d.filter_dictset(TEST_DATA))) == 3


def test_combined_filters():
    # just a tuple
    filter01 = DnfFilters(("name", "==", "Harry Potter"))
    # a tuple in a list
    filter02 = DnfFilters([("name", "==", "Harry Potter")])
    # ANDed conditions
    filter03 = DnfFilters(
        [
            ("name", "like", "%otter%"),
            ("gender", "==", "male"),
            ("age", "<=", 15),
            ("age", ">=", 5),
        ]
    )
    # ANDed conditions - case insensitive LIKE
    filter03a = DnfFilters(
        [
            ("name", "like", "%pOTTER%"),
            ("gender", "==", "male"),
            ("age", "<=", 15),
            ("age", ">=", 5),
        ]
    )
    # ORed conditions
    filter04 = DnfFilters([[("name", "==", "Harry Potter")], [("name", "==", "Hermione Grainger")]])
    # no conditions
    filter05 = DnfFilters()
    # IN conditions
    filter06 = DnfFilters(("name", "in", ["Fleur Isabelle Delacour", "Hermione Grainger"]))
    # contains conditions - on lists
    filter07 = DnfFilters(("affiliations", "contains", "Griffindor"))
    # contains conditions
    filter08 = DnfFilters(("name", "contains", "Isabelle"))

    assert len([a for a in filter01.filter_dictset(TEST_DATA)]) == 1
    assert len([a for a in filter02.filter_dictset(TEST_DATA)]) == 1
    assert len([a for a in filter03.filter_dictset(TEST_DATA)]) == 1
    assert len([a for a in filter03a.filter_dictset(TEST_DATA)]) == 1
    assert len([a for a in filter04.filter_dictset(TEST_DATA)]) == 2
    assert len([a for a in filter05.filter_dictset(TEST_DATA)]) == 6
    assert len([a for a in filter06.filter_dictset(TEST_DATA)]) == 2
    assert len([a for a in filter07.filter_dictset(TEST_DATA)]) == 2
    assert len([a for a in filter08.filter_dictset(TEST_DATA)]) == 1


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
