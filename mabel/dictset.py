"""
DICT(IONARY) (DATA)SET

A group of functions to assist with handling lists of dictionaries, which is
how JSONL files will tend to be handled.

You would use these methods instead of something like Pandas if you are dealing
with potentially huge datasets and only require to be able to do simple
processes on the dataset, row at a time.

Methods provide a limited approximation of SQL functionality:

SELECT   - select_from_dictset
UNION    - union_dictsets
WHERE    - select_from_dictset
JOIN     - join_dictsets
DISTINCT - disctinct
LIMIT    - limit

All methods return generators, which mean they can only be iterated once, to
iterate again they need to be regenerated. create_index is the only exception,
as the index is for look-ups rather than iteration. If you know you're going
to need to iterate more than once, you can use list() or similar to cache the
values, however this may cause problems if the list is large.


Example usage:

# open two jsonl files
list1 = open_dictset_file('list1.jsonl')
list2 = open_dictset_file('list2.jsonl')

# filter the two lists
filtered_list1 = select_from_dictset(list1, ['id', 'name'], lambda x: x['height'] > 2)
filtered_list2 = select_from_dictset(list2, ['id', 'last_seen'])

# join the two lists on id
for record in join_dictsets(filtered_list1, filtered_list2, 'id'):
    print(record)

"""
from typing import Iterator, Any, List, Union, Callable
import json
json_parser: Callable = json.loads
json_dumper: Callable = json.dumps
try:
    import orjson
    json_parser = orjson.loads
    json_dumper = orjson.dumps
except ImportError:
    pass
try:
    import ujson
    json_parser = ujson.loads
except ImportError:
    pass


class JOINS(object):
    INNER_JOIN = 'INNER'
    LEFT_JOIN = 'LEFT'


def select_all(dummy: Any) -> bool:
    """
    Returns True
    """
    return True


def select_record_fields(
        record: dict,
        fields: List[str]) -> dict:
    """
    Selects a subset of fields from a dictionary
    """
    return {k: record.get(k, None) for k in fields}


def order(record: Union[dict, list]) -> Union[dict, list]:
    if isinstance(record, dict):
        return dict(sorted(record.items()))
    if isinstance(record, list):
        return sorted((order(x) for x in record), key=lambda item: '' if not item else item)
    return record


def join_dictsets(
        left: Iterator[dict],
        right: Iterator[dict],
        column: str,
        join_type=JOINS.INNER_JOIN) -> Iterator[dict]:
    """
    Iterates over the left table, matching records fron the right table.

    INNER_JOIN, the default, will discard records unless they appear in both
    tables, LEFT_JOIN will keep all the records fron the left table and add
    records for the right table if a match is found.

    It is recommended that the left table be the larger of the two tables as
    the right table is loaded into memory to perform the matching and look ups.

    NOTES:
    - where columns are in both tables - I don't know what happens.
    - resultant records may have inconsistent columns (same as 
      source lists)

    Approximate SQL:

    SELECT * FROM left JOIN right ON left.column = right.column
    """
    index = create_index(right, column)
    for record in left:
        value = record.get('QID')
        if index.get(value):
            yield {**record, **index[value]}
        elif join_type == JOINS.LEFT_JOIN:
            yield record


def union_dictsets(
        dictset_1: Iterator[dict],
        dictset_2: List[dict]) -> Iterator[dict]:
    """
    Append the records from a set of lists together, doesn't ensure columns
    align.

    Approximate SQL:

    SELECT * FROM dictset_1
    UNION
    SELECT * FROM dictset_2
    """
    for record in dictset_1:
        yield record
    for record in dictset_2:
        yield record


def create_index(
        dictset: Iterator[dict],
        index_column: str) -> dict:
    """
    Create an index of a file to speed up look-ups.
    """
    index = {}
    for record in dictset:
        index_value = record[index_column]
        index[index_value] = record
    return index


def select_from_dictset(
        dictset: Iterator[dict],
        columns: List[str] = ['*'],
        condition: Callable = select_all) -> Iterator[dict]:
    """
    Scan a dictset, filtering rows and selecting columns.

    Basic implementation of SQL SELECT statement for a single table

    Approximate SQL:

    SELECT columns FROM dictset WHERE condition
    """
    for record in dictset:
        if condition(record):
            if columns != ['*']:
                record = select_record_fields(record, columns)
            yield record


def set_column(
        dictset: Iterator[dict],
        column_name: str,
        setter: Callable) -> Iterator[dict]:
    """
    Performs set_value on each row in a set
    """
    for record in dictset:
        yield set_value(record, column_name, setter)


def set_value(
        record: dict,
        column_name: str,
        setter: Callable) -> dict:
    """
    Sets the value of a column to either a fixed value or as the
    result of a function which recieves the row as a parameter
    """
    if callable(setter):
        record[column_name] = setter(record)
    else:
        record[column_name] = setter
    return record


def distinct(
        dictset: Iterator[dict],
        columns: List[str] = ['*']):
    """
    Removes duplicate records from a dictset
    """

    def _noop(x):
        return x
    
    def _filter(x):
        return {k: x.get(k, '') for k in columns}

    seen_hashes: dict = {}
    selector = _noop
    if columns != ['*']:
        selector = _filter

    for record in dictset:
        entry = selector(record)
        entry = json_dumper(entry)
        _hash = hash(entry)
        if seen_hashes.get(_hash):
            continue
        seen_hashes[_hash] = 1
        yield record


def limit(
        dictset: Iterator[dict],
        limit: int):
    """
    Returns up to 'limit' number of records
    """
    counter = limit
    for record in dictset:
        if counter == 0:
            return None
        counter -= 1
        yield record


def dictsets_match(
        dictset_1: Iterator[dict],
        dictset_2: Iterator[dict]):
    """
    Tests if two sets match - this terminates generators
    """
    def _hash_set(dictset: Iterator[dict]):
        xor = 0
        for record in dictset:
            entry = order(record)
            entry = json_dumper(entry)
            _hash = hash(entry)
            xor = xor ^ _hash
        return xor

    return _hash_set(dictset_1) == _hash_set(dictset_2)


def generator_chunker(
        generator: Iterator,
        chunk_size: int) -> Iterator:
    chunk: list = []
    for item in generator:
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = [item]
        else:
            chunk.append(item)
    if chunk:
        yield chunk
