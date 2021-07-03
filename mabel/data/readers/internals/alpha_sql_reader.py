# no-maintain-checks
from moz_sql_parser import parse  # type:ignore
from ..reader import Reader
from ....logging import get_logger
from ...formats import json
from functools import lru_cache
import re

# https://codereview.stackexchange.com/a/248421
_special_regex_chars = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


@lru_cache(4)
def _sql_like_fragment_to_regex(fragment):
    # https://codereview.stackexchange.com/a/36864/229677
    safe_fragment = "".join([_special_regex_chars.get(ch, ch) for ch in fragment])
    return re.compile("^" + safe_fragment.replace("%", ".*?").replace("_", ".") + "$")


def _div(x, y):
    return x / y


def _like(x, y):
    return _sql_like_fragment_to_regex(y).match(str(x))


import operator

# functions which implement the Operators
OPERATORS = {
    "eq": "==",
    "neq": "!=",
    "lt": "<",
    "gt": ">",
    "lte": ">=",
    "gte": "<=",
    "like": "like",
    "add": operator.add,
    "mul": operator.mul,
    "div": _div,
    "sub": operator.sub,
}


def _get_operand(operand):
    if not isinstance(operand, dict):
        # strings are field names
        #        if isinstance(operand, str):
        #            return row[operand]
        # otherwise it's a constant
        return operand
    # string constants are 'literals'
    if "literal" in operand:
        return operand["literal"]
    # some values are handled unusually
    #    value = [row.get(k) for k, v in operand.items() if k in ["timestamp", "text"]]
    #    if len(value):
    #        return value.pop()
    # if we're here, the operand is probably a function
    return _build_filters(operand)


def _build_filters(where_object):
    # None is None
    filters = []
    if where_object is None:
        return None
    # the Operator is the head of the dictionary
    operator = list(where_object.keys())[0]
    if operator in ("and"):
        for predicate in where_object[operator]:
            filters.append(_build_filters(predicate))
        return filters
    if operator in ("or"):
        for predicate in where_object[operator]:
            filters.append([_build_filters(predicate)])
        return filters

    # the operands are the values in for the key
    operands = where_object.get(operator)
    # if the operands are a list, it's the left and right operands
    if isinstance(operands, list):
        left_operand, right_operand = operands[0], operands[1]

    # we build a function, load some of the values and pass it to the Reader
    return (left_operand, OPERATORS.get(operator), _get_operand(right_operand))


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

        # where_statement = _build_where_function(sql.get("where"))
        filters = _build_filters(sql.get("where"))
        print(">>", filters)

        thread_count = 4

        self.reader = Reader(
            #            thread_count=thread_count,
            select=fields,
            filters=filters,
            dataset=table,
            **kwargs,
        )

    def __iter__(self):
        return self

    def __next__(self):
        # deepcode ignore unguarded~next~call: error should bubble
        return next(self.reader)
