from mabel.data.internals.dictset import DictSet
from mabel.data.readers import STORAGE_CLASS
import re
from ..reader import Reader
from ....logging import get_logger

AGGREGATORS = {
    "COUNT": len,
    "MIN": min,
    "MAX": max
}

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

        self.parts = self._split_statement(statement)

        for i, part in enumerate(self.parts):
            if part.upper() == "SELECT":
                self.select = [t.strip() for t in self.parts[i + 1].split(',') if t.strip() != ""]
            if part.upper() == "FROM":
                self._from = self.parts[i + 1].replace(".", "/")
            if part.upper() == "WHERE":
                self.where = self.parts[i + 1]
            if part.upper() == "JOIN":
                raise NotImplementedError("SQL `JOIN` not implemented")
            if part.upper() == "GROUP BY":
                self.group_by = self.parts[i + 1]
                self.select = self._get_aggregators(self.select)
            if part.upper() == "HAVING":
                self.having = self.parts[i + 1]
                raise NotImplementedError("SQL `HAVING` not implemented")
            if part.upper() == "ORDER BY":
                self.order_by = self.parts[i + 1]
                raise NotImplementedError("SQL `ORDER BY` not implemented")
            if part.upper() == "LIMIT":
                self.limit = int(self.parts[i + 1])

    def _get_aggregators(self, aggregators):

        reg = re.compile(r"(\(|\)|,)")

        def inner(aggs):
            for entry in aggs:
                print(entry)
                tokens = [t.strip() for t in reg.split(entry) if t.strip() != ""]

                i = 0
                while i < len(tokens):
                    token = tokens[i]
                    if token.upper() in AGGREGATORS:
                        if len(tokens) < (i + 3):
                            raise ValueError("Not Enough Tokens")
                        if tokens[i+1] == "(" and tokens[i+3] == ")":
                            yield (token.upper(), tokens[i+2])
                        else:
                            raise ValueError("Expecting parenthesis, got `{tokens[i+1]}`, `{}`")
                    i+= 4


        return list(inner(aggregators))


    def _split_statement(self, statement):

        reg = re.compile(
            r"(\bSELECT\b|\bFROM\b|\bJOIN\b|\bWHERE\b|\bGROUP BY\b|\bHAVING\b|\bORDER BY\b|\bLIMIT\b|\-\-|\;)",
            re.IGNORECASE,
        )
        tokens = []

        for line in statement.split("\n"):
            line_tokens = reg.split(line)
            line_tokens = [t.strip() for t in line_tokens if t.strip() != ""]

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

        self.reader = Reader(
            #            thread_count=thread_count,
            query=sql.where,
            dataset=sql._from,
            persistence=STORAGE_CLASS.MEMORY,
            **kwargs,
        )

        if sql.select and not sql.group_by:
            self.reader = self.reader.select(sql.select)

        if sql.group_by:
            groups = self.reader.igroupby(sql.group_by)

            result = []

            for group, data in groups:
                result_row = {}
                result_row[sql.group_by] = group
                for func, column in sql.select:
                    col = data.collect(column)
                    result_row[f"{column}.{func}"] = AGGREGATORS[func](col)
                    result.append(result_row)

            self.reader = DictSet(result)

        if sql.limit:
            self.reader = self.reader.take(sql.limit)

    def __iter__(self):
        return self.reader._iterator
