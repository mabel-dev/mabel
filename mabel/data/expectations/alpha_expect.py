# https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html

from functools import lru_cache
from typing import Any, Iterable
from functools import lru_cache
from ...logging import get_logger
from ...errors import ExpectationNotMetError
import inspect

ExpectationNotMet = Exception


class Expectations(object):

    def __init__(self, set_of_expectations: Iterable[dict]):
        self.set_of_expectations = set_of_expectations
        get_logger().warning("Data Expectations is alpha functionality - it's interface may change and some features may not be supported")

    ###################################################################################
    # COLUMN EXPECTATIONS
    ###################################################################################

    def expect_column_to_exist(self, *, row: dict, column: str, **kwargs):
        if isinstance(row, dict):
            return column in row.keys()
        return False

    def expect_column_values_to_not_be_null(self, *, row: dict, column: str, **kwargs):
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
        raise NotImplementedError()

    def expect_column_values_to_match_regex(
        self, *, row: dict, column: str, regex: str, ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

    def expect_column_values_to_match_like(
        self, *, row: dict, column: str, like: str, ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

    def expect_column_values_length_to_be_be(
        self, *, row: dict, column: str, length: int, ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

    def expect_column_values_length_to_be_between(
        self, *,
        row: dict,
        column: str,
        minimum: int,
        maximum: int,
        ignore_nulls: bool = True, **kwargs):
        raise NotImplementedError()

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
        print(maximum)
        raise NotImplementedError()

    @lru_cache(1)
    def _available_expectations(self):
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
                    print(expectation['expectation'])
                    return False  # data failed to meet expectation
            else:
                return False  # unknown expectation
        return True
