from ..reader import Reader
from ....logging import get_logger


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
                raise NotImplementedError("SQL `GROUP BY` not implemented")
            if part.upper() == "HAVING":
                self.having = self.parts[i + 1]
                raise NotImplementedError("SQL `HAVING` not implemented")
            if part.upper() == "ORDER BY":
                self.order_by = self.parts[i + 1]
                raise NotImplementedError("SQL `ORDER BY` not implemented")
            if part.upper() == "LIMIT":
                self.limit = int(self.parts[i + 1])

    def _split_statement(self, statement):
        import re

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
        return f"< SQL: SELECT ({self.select}) FROM ({self._from}) WHERE ({self.where}) LIMIT ({self.limit}) >"


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
            select=sql.select,
            query=sql.where,
            dataset=sql._from,
            **kwargs,
        )
        if sql.limit:
            self.reader = self.reader.take(sql.limit)

    def __iter__(self):
        return self.reader._iterator
