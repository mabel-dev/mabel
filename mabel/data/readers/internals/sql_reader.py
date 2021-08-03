from ..reader import Reader
from ....logging import get_logger


class SqlParser:
    def __init__(self, statement):
        self.statement = statement

        self.select = ["*"]
        self.dataset = None
        self.query = None

        self.parts = self._split_statement(statement)

        for i, part in enumerate(self.parts):
            if part == "SELECT":
                self.select = [self.parts[i + 1]]
            if part == "FROM":
                self.dataset = self.parts[i + 1].replace(".", "/")
            if part == "WHERE":
                self.query = self.parts[i + 1]

    def _split_statement(self, statement):
        import re

        reg = re.compile(r"(\bSELECT\b|\bFROM\b|\bWHERE\b|\-\-|\;)")
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
        return "< SQL: " + " ".join(self.parts) + " >"


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
            query=sql.query,
            dataset=sql.dataset,
            **kwargs,
        )

    def __iter__(self):
        return self.reader._iterator
