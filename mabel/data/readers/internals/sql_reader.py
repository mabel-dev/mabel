import re
from mabel.data.internals.group_by import GroupBy, AGGREGATORS
from mabel import DictSet
from ..reader import Reader
from ....logging import get_logger


class InvalidSqlError(Exception):
    pass


class SqlParser:
    def __init__(self, statement):
        self.statement = statement

        self.select = ["*"]
        self._from = None
        self.where = None
        self.group_by = None
        self.having = None
        self.order_by = None
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
                self._from = self.parts[i + 1].replace(".", "/")
            elif part.upper() == "WHERE":
                collecting = None
                self.where = self.parts[i + 1]
            elif part.upper() == "JOIN":
                collecting = None
                raise NotImplementedError("SQL `JOIN` not implemented")
            elif re.match("GROUP\sBY", part, re.IGNORECASE):
                collecting = "GROUP BY"
                self.group_by = []
            elif part.upper() == "HAVING":
                collecting = None
                self.having = self.parts[i + 1]
                raise NotImplementedError("SQL `HAVING` not implemented")
            elif part.upper() == "ORDER BY":
                collecting = None
                self.order_by = self.parts[i + 1]
                self._use_threads = True
                raise NotImplementedError("SQL `ORDER BY` not implemented")
            elif part.upper() == "LIMIT":
                collecting = None
                self.limit = int(self.parts[i + 1])

            elif collecting == "SELECT":
                self.select.append(part)

            elif collecting == "GROUP BY":
                self.group_by.append(part)
            
            else:
                InvalidSqlError(f"Unexpected token `{part}`")

        if not self._from:
            raise InvalidSqlError("Queries must always have a FROM statement")
        if self.group_by:
            self.select = self._get_aggregators(self.select)
            self._use_threads = True


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
            raise InvalidSqlError("SELECT must be a set of aggregation functions (e.g. COUNT, SUM) when using GROUP BY")
        
        return aggs

    def _split_statement(self, statement):

        reg = re.compile(
            r"(\bSELECT\b|\bFROM\b|\bJOIN\b|\bWHERE\b|\bGROUP\sBY\b|\bHAVING\b|\bORDER\sBY\b|\bLIMIT\b|,|\-\-|\;)",
            re.IGNORECASE,
        )
        tokens = []

        for line in statement.split("\n"):
            line_tokens = reg.split(line)
            line_tokens = [t.strip() for t in line_tokens if t.strip() != "" and t.strip() != ","]

            # remove anything after the comments flag
            while "--" in line_tokens:
                line_tokens = line_tokens[: line_tokens.index("--")]

            tokens += line_tokens

        return tokens

    def __repr__(self):
        return f"< SQL: SELECT ({self.select}) FROM ({self._from}) WHERE ({self.where}) GROUP BY ({self.group_by}) LIMIT ({self.limit}) >"


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

        # if the function forces the order of the results, use threads to
        # speed up the read of the data
        thread_count = None
        if sql._use_threads:
            thread_count = 2

        self.reader = Reader(
            thread_count=thread_count,
            query=sql.where,
            dataset=sql._from,
            **kwargs,
        )

        #
        if sql.select and not sql.group_by and not sql.select == ['*']:
            self.reader = self.reader.select(sql.select)

        if sql.group_by:

            groups = GroupBy(self.reader, *sql.group_by).aggregate(sql.select)
            self.reader = DictSet(groups)

        if sql.limit:
            self.reader = self.reader.take(sql.limit)

    def __iter__(self):
        return self.reader._iterator
