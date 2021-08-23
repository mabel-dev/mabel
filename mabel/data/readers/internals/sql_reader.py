# no-maintain-checks
import re
from mabel.data.internals.group_by import GroupBy, AGGREGATORS
from mabel import DictSet
from ..reader import Reader
from ....logging import get_logger
from .sql_functions import *

# not all are implemented
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
    r"TOP"
]

class InvalidSqlError(Exception):
    pass

def safe_get(arr, index, default=None):
    return arr[index] if index < len(arr) else default

def remove_comments(string):
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE|re.DOTALL)
    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return "" # so we will return empty to remove the comment
        else: # otherwise, we will return the 1st group
            return match.group(1) # captured quoted-string
    return regex.sub(_replacer, string)

class SqlParser:
    def __init__(self, statement):

        self.select = ["*"]
        self._from = None
        self.where = None
        self.group_by = None
        self.having = None
        self.order_by = None
        self.order_by_desc = False
        self.limit = None

        self._use_threads = False

        self.statement = remove_comments(statement)
        self.parts = self._split_statement(self.statement)

        collecting = None

        for i, part in enumerate(self.parts):

            if part.upper() == "SELECT":
                collecting = "SELECT"
                self.select = []
            elif part.upper() == "FROM":
                collecting = None
                self._from = safe_get(self.parts, i + 1, '').replace(".", "/")
            elif part.upper() == "WHERE":
                collecting = None
                self.where = self.parts[i + 1]
            elif part.upper() == "JOIN":
                collecting = None
                raise NotImplementedError("SQL `JOIN` not implemented")
            elif re.match(r"GROUP\sBY", part, re.IGNORECASE):
                collecting = "GROUP BY"
                self.group_by = []
            elif part.upper() == "HAVING":
                collecting = None
                self.having = safe_get(self.parts, i + 1, '')
                raise NotImplementedError("SQL `HAVING` not implemented")
            elif re.match(r"ORDER\sBY", part, re.IGNORECASE):
                collecting = None
                self.order_by = safe_get(self.parts, i + 1, '')
                self.order_by_desc = safe_get(self.parts, i + 2, '').upper() == "DESC"
                self._use_threads = True
            elif part.upper() == "LIMIT":
                collecting = None
                self.limit = int(safe_get(self.parts, i + 1, '100'))

            elif collecting == "SELECT":
                self.select.append(part)

            elif collecting == "GROUP BY":
                self.group_by.append(part)

            else:
                InvalidSqlError(f"Unexpected token `{part}`")

        if not self._from:
            raise InvalidSqlError("Queries must always have a FROM statement")
        if self.group_by or "(" in ''.join(self.select):
            self.select = self._get_functions(self.select)
            self._use_threads = True
        if "(" in ''.join(self.group_by or []):
            self.group_by = self._get_functions(self.group_by)
        if len(self.select) > 1 and any([f[0].upper() == "COUNT" for f in self.select if isinstance(self.select, tuple)]) and not self.group_by:
            raise InvalidSqlError("`SELECT COUNT(*)` must be the only SELECT statement if COUNT(*) is used without a GROUP BY")

    def _get_functions(self, aggregators):

        reg = re.compile(r"(\(|\)|,)")
        KNOWN_FUNCTIONS = list(AGGREGATORS.keys()) + list(FUNCTIONS.keys())

        def inner(aggs):
            for entry in aggs or []:
                tokens = [t.strip() for t in reg.split(entry) if t.strip() != ""]

                i = 0
                while i < len(tokens):
                    token = tokens[i]
                    if safe_get(tokens, i+1) != "(":
                        yield token
                        i += 1
                    elif token.upper() in KNOWN_FUNCTIONS:
                        if len(tokens) < (i + 3):
                            raise InvalidSqlError("SELECT statement terminated before it was complete.")
                        if tokens[i + 1] == "(" and tokens[i + 3] == ")":
                            yield (token.upper(), tokens[i + 2])
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
            r"(" + r''.join([r"\b" + i + r"\b|" for i in SQL_KEYWORDS]) + r",|\-\-|\;)",
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
        return f"< SQL: SELECT ({self.select})\nFROM ({self._from})\nWHERE ({self.where})\nGROUP BY ({self.group_by})\nORDER BY ({self.order_by})\nLIMIT ({self.limit}) >"


def apply_functions_on_read_thru(ds, functions, merge=False):
    for record in ds:
        if merge:
            result_record = record
        else:
            result_record = {}
        for function in functions:
            if isinstance(function, str) and not merge:
                result_record[function] = record.get(function)
            else:
                result_record[f"{function[0]}({function[1]})"] = FUNCTIONS[function[0]](record.get(function[1]))
        yield result_record


class SqlReader:
    def __init__(self, sql_statement: str, **kwargs):
        """
        Use basic SQL queries to filter Reader.

        Parameters:
            sql_statement: string
            kwargs: parameters to pass to the Reader

        Note:
            `select` is taken from SQL SELECT
            `dataset` is taken from SQL FROM
            `query` is taken from SQL WHERE
        """
        sql = SqlParser(sql_statement)
        get_logger().info(repr(sql).replace('\n', ' '))

        if sql._use_threads:
            kwargs["fork_processes"] = True

        self.reader = Reader(
            query=sql.where,
            dataset=sql._from,
            **kwargs,
        )

        if sql.select == [("COUNT", "*")] and not sql.group_by:
            count = -1
            for count, r in enumerate(self.reader):
                pass
            self.reader = DictSet([{"COUNT(*)": count + 1}])
        elif not sql.group_by and any(isinstance(selector, tuple) for selector in sql.select):
            self.reader = DictSet(apply_functions_on_read_thru(self.reader, sql.select))
        elif sql.select and not sql.group_by and not sql.select == ["*"]:
            self.reader = self.reader.select(sql.select)
        elif sql.group_by:
            if not all(isinstance(sql.select, tuple) for sql.select in sql.select):
                raise InvalidSqlError(
                    "SELECT must be a set of aggregation functions (e.g. COUNT, SUM) when using GROUP BY"
                )

            if any(isinstance(selector, tuple) for selector in sql.group_by):
                self.reader = apply_functions_on_read_thru(self.reader, sql.group_by, True)
                sql.group_by = [ f"{group[0]}({group[1]})" if isinstance(group, tuple) else group for group in sql.group_by]
            
            groups = GroupBy(self.reader, *sql.group_by).aggregate(sql.select)
            self.reader = DictSet(groups)
        if sql.order_by:
            take = 10000
            if sql.limit:
                take = sql.limit
            self.reader = DictSet(self.reader.sort_and_take(column=sql.order_by, take=take, descending=sql.order_by_desc))
        if sql.limit:
            self.reader = self.reader.take(sql.limit)

    def __iter__(self):
        return self.reader._iterator
