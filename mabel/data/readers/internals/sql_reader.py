import re

import simdjson
from ....data.internals.group_by import GroupBy, AGGREGATORS
from ..reader import Reader
from ....logging import get_logger
from .inline_functions import *
from .inline_evaluator import Evaluator

# not all are implemented, but we split by reserved words anyway
SQL_KEYWORDS = [
    r"SELECT",
    r"FROM",
    r"JOIN",
    r"WHERE",
    r"GROUP\sBY",
    r"HAVING",
    r"ORDER\sBY",
    r"LIMIT",
    r"DESC",
    r"INNER",
    r"OUTER",
    r"IN",
    r"BETWEEN",
    r"DISTINCT",
    r"ASC",
    r"TOP",
]


class InvalidSqlError(Exception):
    pass


def safe_get(arr, index, default=None):
    return arr[index] if index < len(arr) else default


def remove_comments(string):
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        else:  # otherwise, we will return the 1st group
            return match.group(1)  # captured quoted-string

    return regex.sub(_replacer, string)


class SqlParser:
    def __init__(self, statement):

        self.select = ["*"]
        self.distinct = False
        self._from = None
        self.where = None
        self.group_by = None
        self.order_by = None
        self.order_by_desc = False
        self.limit = None

        # get rid of comments as early as possible
        self.statement = remove_comments(statement)
        # split the statement into it's parts
        self.parts = self._split_statement(self.statement)

        collecting = None

        for i, part in enumerate(self.parts):

            if part.upper() == "SELECT":
                collecting = "SELECT"
                self.select = []
            elif part.upper() == "FROM":
                collecting = None
                self._from = safe_get(self.parts, i + 1, "").replace(".", "/")
            elif part.upper() == "WHERE":
                collecting = None
                self.where = self.parts[i + 1]
            elif re.match(r"GROUP\sBY", part, re.IGNORECASE):
                collecting = "GROUP BY"
                self.group_by = []
            elif re.match(r"ORDER\sBY", part, re.IGNORECASE):
                collecting = None
                self.order_by = safe_get(self.parts, i + 1, "")
                self.order_by_desc = safe_get(self.parts, i + 2, "").upper() == "DESC"
            elif part.upper() == "LIMIT":
                collecting = None
                self.limit = int(safe_get(self.parts, i + 1, "100"))

            elif collecting == "SELECT":
                self.select.append(part)

            elif collecting == "GROUP BY":
                self.group_by.append(part)

            else:
                InvalidSqlError(f"Unexpected token `{part}`")

        if not self._from:
            raise InvalidSqlError("Queries must always have a FROM statement")
        if self.group_by or "(" in "".join(self.select):
            self.select = self._get_functions(self.select)
        if "(" in "".join(self.group_by or []):
            self.group_by = self._get_functions(self.group_by)
        if (
            len(self.select) > 1
            and any(
                [
                    f[0].upper() == "COUNT"
                    for f in self.select
                    if isinstance(self.select, tuple)
                ]
            )
            and not self.group_by
        ):
            raise InvalidSqlError(
                "`SELECT COUNT(*)` must be the only SELECT statement if COUNT(*) is used without a GROUP BY"
            )
        if isinstance(self.select[0], str) and self.select[0].upper() == "DISTINCT":
            self.distinct = True
            self.select.pop(0)

    def _get_functions(self, aggregators):

        reg = re.compile(r"(\(|\)|,)")
        KNOWN_FUNCTIONS = list(AGGREGATORS.keys()) + list(FUNCTIONS.keys())

        def inner(aggs):
            for entry in aggs or []:
                tokens = [t.strip() for t in reg.split(entry) if t.strip() != ""]

                i = 0
                while i < len(tokens):
                    token = tokens[i]
                    if safe_get(tokens, i + 1) != "(":
                        yield token
                        i += 1
                    elif token.upper() in KNOWN_FUNCTIONS:
                        if len(tokens) < (i + 3):
                            raise InvalidSqlError(
                                "SELECT statement terminated before it was complete."
                            )
                        if tokens[i + 1] == "(" and safe_get(tokens, i + 3) == ")":
                            yield (token.upper(), safe_get(tokens, i + 2))
                        else:
                            raise InvalidSqlError(
                                f"Expecting parenthesis, got `{tokens[i+1]}`, `{tokens[i+3]}`."
                            )
                        i += 4
                    else:
                        raise InvalidSqlError(f"Unrecognised SQL function `{token}`.")

        aggs = list(inner(aggregators))

        return aggs

    def _split_statement(self, statement):

        reg = re.compile(
            r"(" + r"".join([r"\b" + i + r"\b|" for i in SQL_KEYWORDS]) + r",|\-\-|\;)",
            re.IGNORECASE,
        )
        tokens = []

        for line in statement.split("\n"):
            line_tokens = reg.split(line)
            line_tokens = [
                t.strip() for t in line_tokens if t.strip() != "" and t.strip() != ","
            ]

            # remove anything after the comments flag
            while "--" in line_tokens:
                line_tokens = line_tokens[: line_tokens.index("--")]

            tokens += line_tokens

        return tokens

    def __repr__(self):
        return f"< SQL: SELECT {'DISTINCT ' if self.distinct else ''}({self.select})\nFROM ({self._from})\nWHERE ({self.where})\nGROUP BY ({self.group_by})\nORDER BY ({self.order_by})\nLIMIT ({self.limit}) >"


def apply_functions_on_read_thru(ds, functions, merge=False):
    """
    Read-thru the dataset, performing record-level functions such as YEAR.

    Parameters:
        ds: Iterable of Dicts
        functions: Iterable of functions
        merge: Boolean (optional)
            If True add function results to the record, otherwise the record
            is just the result of the functions.
    """
    for record in ds:
        if merge:
            if isinstance(record, simdjson.Object):
                record = record.as_dict()
            result_record = record
        else:
            result_record = {}
        for function in functions:
            if isinstance(function, str) and not merge:
                result_record[function] = record.get(function)
            else:
                result_record[f"{function[0]}({function[1]})"] = FUNCTIONS[function[0]](
                    record.get(function[1])
                )
        yield result_record


def SqlReader(sql_statement: str, **kwargs):
    """
    Use basic SQL queries to filter Reader.

    Parameters:
        sql_statement: string
        kwargs: parameters to pass to the Reader

    Note:
        `select` is taken from SQL SELECT
        `dataset` is taken from SQL FROM
        `filters` is taken from SQL WHERE
    """
    from mabel import DictSet

    sql = SqlParser(sql_statement)
    get_logger().info(repr(sql).replace("\n", " "))

    reader = Reader(
        filters=sql.where,
        dataset=sql._from,
        **kwargs,
    )

    # if the query is COUNT(*) on a SELECT, just do it.
    if sql.select == [("COUNT", "*")] and not sql.group_by:
        count = -1
        for count, r in enumerate(reader):
            pass
        reader = DictSet(iter([{"COUNT(*)": count + 1}]))
    # if we're not grouping and we have functions, execute them
    elif not sql.group_by and any(
        isinstance(selector, tuple) for selector in sql.select
    ):
        reader = DictSet(apply_functions_on_read_thru(reader, sql.select))
    # if we are selecting columns and we're not a group_by, select the columns
    elif sql.select and not sql.group_by and not sql.select == ["*"]:
        reader = reader.select(sql.select)
    # if we're doing a group-by
    elif sql.group_by:
        if not all(isinstance(select, tuple) for select in sql.select):
            raise InvalidSqlError(
                "SELECT must be a set of aggregation functions (e.g. COUNT, SUM) when using GROUP BY"
            )
        # if we're grouping by a function result (like YEAR), calculate that function
        if any(isinstance(selector, tuple) for selector in sql.group_by):
            reader = apply_functions_on_read_thru(reader, sql.group_by, True)
            sql.group_by = [
                f"{group[0]}({group[1]})" if isinstance(group, tuple) else group
                for group in sql.group_by
            ]

        groups = GroupBy(reader, *sql.group_by).aggregate(sql.select)
        reader = DictSet(groups)
    if sql.distinct:
        reader = reader.distinct()
    if sql.order_by:
        take = 10000
        if sql.limit:
            take = sql.limit
        reader = DictSet(
            reader.sort_and_take(
                column=sql.order_by, take=take, descending=sql.order_by_desc
            )
        )
    if sql.limit:
        reader = reader.take(sql.limit)

    return reader
