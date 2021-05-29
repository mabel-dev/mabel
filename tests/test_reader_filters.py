import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk import DiskReader
from mabel.data.readers.internals.filters import Filters
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
    r = Reader(
        inner_reader=DiskReader, dataset="tests/data/tweets/", raw_path=True, filters=[]
    )
    for index, item in enumerate(r):
        pass
    assert index == 49, index


def test_reader_filters_single_filter():
    """ensure the reader filter is working as expected"""
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        raw_path=True,
        filters=[("username", "==", "NBCNews")],
    )

    for index, item in enumerate(r):
        pass
    assert index == 43, index


def test_reader_filters_multiple_filter():
    """ensure the reader filter is working as expected"""
    r = Reader(
        inner_reader=DiskReader,
        dataset="tests/data/tweets/",
        raw_path=True,
        filters=[
            ("username", "==", "NBCNews"),
            ("timestamp", ">=", "2020-01-12T07:11:04"),
        ],
    )
    for index, item in enumerate(r):
        pass
    assert index == 33, index


def test_filters():

    d = Filters(filters=[("age", "==", 11), ("gender", "in", ("a", "b", "male"))])
    assert len(list(d.filter_dictset(TEST_DATA))) == 1


def test_empty_filters():

    d = Filters(filters=None)
    assert len(list(d.filter_dictset(TEST_DATA))) == 6


def test_like_filters():

    d = Filters(filters=[("dob", "like", "%-12-%")])
    assert len(list(d.filter_dictset(TEST_DATA))) == 3


def test_combined_filters():

    # just a tuple
    filter01 = Filters(("name", "==", "Harry Potter"))
    # a tuple in a list
    filter02 = Filters([("name", "==", "Harry Potter")])
    # ANDed conditions
    filter03 = Filters(
        [
            ("name", "like", "%otter%"),
            ("gender", "==", "male"),
            ("age", "<=", 15),
            ("age", ">=", 5),
        ]
    )
    # ANDed conditions - case insensitive LIKE
    filter03a = Filters(
        [
            ("name", "like", "%pOTTER%"),
            ("gender", "==", "male"),
            ("age", "<=", 15),
            ("age", ">=", 5),
        ]
    )
    # ORed conditions
    filter04 = Filters(
        [[("name", "==", "Harry Potter")], [("name", "==", "Hermione Grainger")]]
    )
    # no conditions
    filter05 = Filters()
    # IN conditions
    filter06 = Filters(("name", "in", ["Fleur Isabelle Delacour", "Hermione Grainger"]))
    # contains conditions - on lists
    filter07 = Filters(("affiliations", "contains", "Griffindor"))
    # contains conditions
    filter08 = Filters(("name", "contains", "Isabelle"))

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
    test_reader_filters_no_filter()
    test_reader_filters_single_filter()
    test_reader_filters_multiple_filter()
    test_filters()
    test_empty_filters()
    test_like_filters()
    test_combined_filters()

    print("okay")
