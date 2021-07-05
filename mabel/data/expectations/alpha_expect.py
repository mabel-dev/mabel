"""
Inspired by the Great Expectations library.

Rather than testing for conformity through defining a schema, expectations are a set
of assertions we can apply to our data.

Whilst a schema-based approach isn't exactly procedural, expectations are a more
declarative way to define valid data.

These assertions can also define a schema (we can expect a set of columns, each with
an expected type), but they also allow us to have more complex assertions, such as
the values in a set of columns should add to 100, or the values in a column are
increasing.

This is designed to be applied to streaming data as each record passes through a point
in a flow - as such it is not intended to test an entire dataset at once to test its
validity, and some assertions are impractical - for example an expectation of the mean
of all of the values in a table.

- if data doesn't match, I'm not cross, I'm just disappointed.
"""
from mabel.data.expectations.internals.text import build_regex
import re
import inspect
from functools import lru_cache
from typing import Any, Iterable
from functools import lru_cache
from ...logging import get_logger
from ...errors import ExpectationNotMetError
from .internals import sql_like_to_regex


class Expectations(object):

    def __init__(self, set_of_expectations: Iterable[dict]):
        self.set_of_expectations = set_of_expectations
        get_logger().warning("Data Expectations is alpha functionality - it's interface may change and some features may not be supported")

    ###################################################################################
    # COLUMN EXPECTATIONS
    ###################################################################################

    def expect_column_to_exist(self, *, row: dict, column: str, **kwargs):
        """ Confirms that a named column exists. """
        if isinstance(row, dict):
            return column in row.keys()
        return False

    def expect_columns_to_match_set(self, *, row: dict, columns, ignore_excess: bool = True, **kwargs):
        """
        Confirms that the columns in a record matches a given set.

        Ignore_excess, ignore columns not on the list, set to False to test against a
        fixed set.
        """
        if ignore_excess:
            return all(key in columns for key in row.keys())
        else:
            return sorted(columns) == sorted(list(row.keys()))


    def expect_column_values_to_not_be_null(self, *, row: dict, column: str, **kwargs):
        """ Confirms the value in a column is not null """
        return row.get(column) is not None

    def expect_column_values_to_be_of_type(
        self, *, row: dict, column: str, expected_type, ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return type(value).__name__ == expected_type
        return ignore_nulls

    def expect_column_values_to_be_in_type_list(
        self, *, row: dict, column: str, type_list: Iterable, ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return type(value).__name__ in type_list
        return ignore_nulls

    def expect_column_values_to_be_unique(
        self, *, row: dict, column: str, ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

    def expect_column_values_to_be_between(
        self, *, row: dict, column: str, minimum, maximum, ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return value >= minimum and value <= maximum
        return ignore_nulls

    def expect_column_values_to_be_increasing(
        self, *, row: dict, column: str, ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

    def expect_column_values_to_be_decreasing(
        self, *, row: dict, column: str, ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

    def expect_column_values_to_be_in_set(
        self, *, row: dict, column: str, symbols: Iterable, ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return value in symbols
        return ignore_nulls

    def expect_column_values_to_match_regex(
        self, *, row: dict, column: str, regex: str, ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return build_regex(regex).match(str(value)) is not None
        return ignore_nulls

    def expect_column_values_to_match_like(
        self, *, row: dict, column: str, like: str, ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return sql_like_to_regex(like).match(str(value)) is not None
        return ignore_nulls

    def expect_column_values_length_to_be_be(
        self, *, row: dict, column: str, length: int, ignore_nulls: bool = True, **kwargs):
        """ Confirms the string length of the value in a column is a given length """
        value = row.get(column)
        if value:
            return len(value) == length
        return ignore_nulls

    def expect_column_values_length_to_be_between(
        self, *,
        row: dict,
        column: str,
        minimum: int,
        maximum: int,
        ignore_nulls: bool = True, **kwargs):
        value = row.get(column)
        if value:
            return len(value) >= minimum and len(value) <= maximum
        return ignore_nulls

    ###################################################################################
    # TABLE EXPECTATIONS
    ###################################################################################

    def expect_table_row_count_to_be(self, *, dataset, count: int, **kwargs):
        raise NotImplementedError()

    def expect_table_row_count_to_be_between(self, *, dataset, minimum: int, maximum: int, **kwargs):
        raise NotImplementedError()

    def expect_table_row_count_to_be_more_than(self, *, dataset, minimum: int, **kwargs):
        raise NotImplementedError()

    def expect_table_row_count_to_be_less_than(self, *, dataset, maximum: int, **kwargs):
        raise NotImplementedError()

    @lru_cache(1)
    def _available_expectations(self):
        """
        Programatically get the list of expectations and build them into a dictionary.
        We then use this dictionary to look up the methods to test the expectations in
        the set of expectations for a dataset.
        """
        expectations = {}
        for handle, member in inspect.getmembers(self):
            if callable(member) and handle.startswith("expect_"):
                expectations[handle] = member
        return expectations

    def test_record(self, record):
        full_suite = self._available_expectations()
        for expectation in self.set_of_expectations:
            if expectation['expectation'] in full_suite:
                if not full_suite[expectation['expectation']](row=record, **expectation):
                    #print(expectation['expectation'])
                    return False  # data failed to meet expectation
            else:
                return False  # unknown expectation
        return True
