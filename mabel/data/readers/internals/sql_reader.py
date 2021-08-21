# no-maintain-checks
import re
from mabel.data.internals.group_by import GroupBy, AGGREGATORS
from mabel import DictSet
from ..reader import Reader
from ....logging import get_logger


class InvalidSqlError(Exception):
    pass

def safe_get(arr, index, default=None):
    return arr[index] if index < len(arr) else default

class SqlParser:
    def __init__(self, statement):
        self.statement = statement

        self.select = ["*"]
        self._from = None
        self.where = None
        self.group_by = None
        self.having = None
        self.order_by = None
        self.order_by_desc = False
        self.limit = None

        self._use_threads = False

        self.parts = self._split_statement(statement)

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
            self.select = self._get_aggregators(self.select)
            self._use_threads = True
        if len(self.select) > 1 and any([f[0].upper() == "COUNT" for f in self.select if isinstance(self.select, tuple)]) and not self.group_by:
            raise InvalidSqlError("`SELECT COUNT(*)` must be the only SELECT statement if COUNT(*) is used without a GROUP BY")

    def _get_aggregators(self, aggregators):

        reg = re.compile(r"(\(|\)|,)")

        def inner(aggs):
            for entry in aggs or []:
                tokens = [t.strip() for t in reg.split(entry) if t.strip() != ""]

                i = 0
                while i < len(tokens):
                    token = tokens[i]
                    if token.upper() in AGGREGATORS:
                        if len(tokens) < (i + 3):
                            raise ValueError("Not Enough Tokens")
                        if tokens[i + 1] == "(" and tokens[i + 3] == ")":
                            yield (token.upper(), tokens[i + 2])
                        else:
                            raise ValueError(
                                "Expecting parenthesis, got `{tokens[i+1]}`, `{}`"
                            )
                    i += 4

        aggs = list(inner(aggregators))
        if len(aggs) == 0:
            raise InvalidSqlError("SELECT cannot be `*` when using GROUP BY")
        if not all(isinstance(agg, tuple) for agg in aggs):
            raise InvalidSqlError(
                "SELECT must be a set of aggregation functions (e.g. COUNT, SUM) when using GROUP BY"
            )

        return aggs

    def _split_statement(self, statement):

        reg = re.compile(
            r"(\bSELECT\b|\bFROM\b|\bJOIN\b|\bWHERE\b|\bGROUP\sBY\b|\bHAVING\b|\bORDER\sBY\b|\bLIMIT\b|\bDESC\b|,|\-\-|\;)",
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
        get_logger().debug(sql)

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
        elif sql.select and not sql.group_by and not sql.select == ["*"]:
            self.reader = self.reader.select(sql.select)
        elif sql.group_by:
            groups = GroupBy(self.reader, *sql.group_by).aggregate(sql.select)
            self.reader = DictSet(groups)
        if sql.order_by:
            take = 5000
            if sql.limit:
                take = sql.limit
            self.reader = DictSet(self.reader.sort_and_take(column=sql.order_by, take=take, descending=sql.order_by_desc))
        if sql.limit:
            self.reader = self.reader.take(sql.limit)

    def __iter__(self):
        return self.reader._iterator
