# no-maintain-checks
from moz_sql_parser import parse  # type:ignore
from ..reader import Reader
from ....logging import get_logger
from ...formats import json
from functools import lru_cache
import re
import os

# https://codereview.stackexchange.com/a/248421
_special_regex_chars = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


@lru_cache(4)
def _sql_like_fragment_to_regex(fragment):
    # https://codereview.stackexchange.com/a/36864/229677
    safe_fragment = "".join([_special_regex_chars.get(ch, ch) for ch in fragment])
    return re.compile("^" + safe_fragment.replace("%", ".*?").replace("_", ".") + "$")


def _eq(x, y):
    return x == y


def _neq(x, y):
    return x != y


def _lt(x, y):
    return x < y


def _gt(x, y):
    return x > y


def _lte(x, y):
    return x <= y


def _gte(x, y):
    return x >= y


def _and(x, y):
    return x and y


def _or(x, y):
    return x or y


def _add(x, y):
    return x + y


def _mul(x, y):
    return x * y


def _div(x, y):
    return x / y


def _sub(x, y):
    return x - y


def _like(x, y):
    return _sql_like_fragment_to_regex(y).match(str(x))


# functions which implement the Operators
OPERATORS = {
    "eq": _eq,
    "neq": _neq,
    "lt": _lt,
    "gt": _gt,
    "lte": _lte,
    "gte": _gte,
    "and": _and,
    "or": _or,
    "like": _like,
    "add": _add,
    "mul": _mul,
    "div": _div,
    "sub": _sub,
}


def _get_operand(operand, row):
    if not isinstance(operand, dict):
        # strings are field names
        if isinstance(operand, str):
            return row[operand]
        # otherwise it's a constant
        return operand
    # string constants are 'literals'
    if "literal" in operand:
        return operand["literal"]
    # some values are handled unusually
    value = [row.get(k) for k, v in operand.items() if k in ["timestamp", "text"]]
    if len(value):
        return value.pop()
    # if we're here, the operand is probably a function
    return _build_where_function(operand)(row)


def _build_where_function(where_object):
    # None is None
    if where_object is None:
        return None
    # the Operator is the head of the dictionary
    operator = list(where_object.keys())[0]
    # the operands are the values in for the key
    operands = where_object.get(operator)
    # if the operands are a list, it's the left and right operands
    if isinstance(operands, list):
        left_operand, right_operand = operands[0], operands[1]
    # if it's a dict, run the Operator against it
    elif isinstance(operands, dict):
        return lambda row: _get_operand(operands, row)

    # we build a function, load some of the values and pass it to the Reader
    return SqlOperator(operator, left_operand, right_operand)


class SqlOperator:
    """
    This is implemented as a class so it can be picked so it can be used with
    the multi-processing library via the fork_processes option in the Reader.
    """

    __slots__ = ("left_operand", "right_operand", "operation", "__name__")

    def __init__(self, operator, left_operand, right_operand):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operation = OPERATORS.get(operator)
        self.__name__ = f"SQL_WHERE('{operator}')"

    def __call__(self, row):
        _left_operand = _get_operand(self.left_operand, row)
        _right_operand = _get_operand(self.right_operand, row)
        return self.operation(_left_operand, _right_operand)


class SqlReader:
    def __init__(self, sql_statement: str, **kwargs):
        """
        THIS IS ALPHA - interface and features subject to change

        Use basic SQL queries to filter Reader.

        Parameters:
            sql_statement: string
            kwargs: parameters to pass to the Reader

        Note:
            `select` is taken from SQL SELECT
            `dataset` is taken from SQL FROM
            `where` is taken from SQL WHERE
        """
        get_logger().warning(
            "SQL Reader is Alpha - interface and features subject to change"
        )

        sql = parse(sql_statement)
        get_logger().debug(json.serialize(sql))

        field_list = sql.get("select")
        fields = []
        if isinstance(field_list, str):
            if field_list == "*":
                field_list = {"value": "*"}
            field_list = [field_list]
        elif not isinstance(field_list, list):
            field_list = [field_list]
        fields = [field.get("value") for field in field_list]

        table = sql.get("from").replace(".", "/")

        where_statement = _build_where_function(sql.get("where"))

        thread_count = 4

        self.reader = Reader(
            thread_count=thread_count,
            select=fields,
            where=where_statement,
            dataset=table,
            **kwargs,
        )

    def __iter__(self):
        return self

    def __next__(self):
        # deepcode ignore unguarded~next~call: error should bubble
        return next(self.reader)
