import re
from typing import Optional
from ....data.readers.internals.inline_evaluator import Evaluator
from ....utils.token_labeler import TOKENS, Tokenizer
from ....logging import get_logger


SQL_PARTS = [
    r"SELECT",
    r"DISTINCT",
    r"FROM",
    r"WHERE",
    r"GROUP\sBY",
    r"HAVING",
    r"ORDER\sBY",
    r"ASC",
    r"DESC",
    r"LIMIT",
]


class InvalidSqlError(Exception):
    pass


class SqlParser:
    def __init__(self, statement):
        self.select_expression: Optional[str] = None
        self.select_evaluator: None
        self.distinct: bool = False
        self.dataset: Optional[str] = None
        self.where_expression: Optional[str] = None
        self.group_by: Optional[str] = None
        self.having: Optional[str] = None
        self.order_by: Optional[str] = None
        self.order_descending: bool = False
        self.limit: Optional[int] = None

        self.parse(statement=statement)
        self.select_evaluator = Evaluator(self.select_expression)

    def __repr__(self):
        return str(
            {
                "select": self.select_expression,
                "disctinct": self.distinct,
                "from": self.dataset,
                "where": self.where_expression,
                "group by": self.group_by,
                "having": self.having,
                "order by": self.order_by,
                "descending": self.order_descending,
                "limit": self.limit,
            }
        )

    def sql_parts(self, string):
        reg = re.compile(
            r"(" + r"|".join([r"\b" + i + r"\b" for i in SQL_PARTS]) + r")",
            re.IGNORECASE,
        )
        parts = reg.split(string)
        return [part.strip() for part in parts if part.strip() != ""]

    def clean_statement(self, string):
        """
        Remove carriage returns and all whitespace to single spaces
        """
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        return _RE_COMBINE_WHITESPACE.sub(" ", string).strip()

    def remove_comments(self, string):
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

    def validate_dataset(self, dataset):
        """
        The from clause is used to address resources in data storage; to prevent
        abusing this, we whitelist valid characters.

        Note that when this information is used, it is used in a glob query and
        only resources that exist are referenced with a reader, this should prevent
        exec flaws - but may still result in IDOR. To reduce this, use the
        `valid_dataset_prefixes` parameter on the Reader.
        """
        # start with a letter
        if not dataset[0].isalpha():
            raise InvalidSqlError("Malformed FROM clause - must start with a letter.")
        # can't be attempting path traversal
        if ".." in dataset or "//" in dataset or "--" in dataset:
            raise InvalidSqlError(
                "Malformed FROM clause - invalid repeated characters."
            )
        # can only contain limited character set (alpha num . / - _)
        if (
            not dataset.replace(".", "")
            .replace("/", "")
            .replace("-", "")
            .replace("_", "")
            .isalnum()
        ):
            raise InvalidSqlError("Malformed FROM clause - invalid characters.")
        return True

    def parse(self, statement):
        # clean the string
        clean = self.remove_comments(statement)
        clean = self.clean_statement(clean)
        # split into bits by SQL keywords
        parts = self.sql_parts(clean)
        # put into a token labeller
        labeler = Tokenizer(parts)

        while labeler.has_next():
            if labeler.peek().upper() == "SELECT":
                labeler.next()
                if labeler.next_token_value().upper() == "DISTINCT":
                    self.distinct = True
                    labeler.next()
                self.select_expression = labeler.peek()
            if labeler.peek().upper() == "FROM":
                labeler.next()
                self.dataset = labeler.peek().replace(".", "/")
            if labeler.peek().upper() == "WHERE":
                labeler.next()
                self.where_expression = labeler.peek()
            if labeler.peek().upper() == "GROUP BY":
                labeler.next()
                self.group_by = labeler.peek()
            if labeler.peek().upper() == "HAVING":
                labeler.next()
                self.having = labeler.peek()
            if labeler.peek().upper() == "ORDER BY":
                labeler.next()
                self.order_by = labeler.next()
                if labeler.has_next() and labeler.next_token_value().upper() in (
                    "ASC",
                    "DESC",
                ):
                    self.order_descending = labeler.next_token_value().upper() == "DESC"
            if labeler.peek().upper() == "LIMIT":
                labeler.next()
                self.limit = int(labeler.peek())
            labeler.next()

        # validate inputs (currently only the FROM clause)
        self.validate_dataset(self.dataset)


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

    # some imports here to remove cyclic imports
    from mabel import DictSet, Reader
    from mabel.data import STORAGE_CLASS

    sql = SqlParser(sql_statement)
    get_logger().info(repr(sql))

    actual_select = sql.select_expression
    if sql.select_expression is None:
        actual_select = "*"
    elif sql.select_expression != "*":
        actual_select = sql.select_expression + ",*"

    reader = Reader(
        select=actual_select,
        dataset=sql.dataset,
        filters=sql.where_expression,
        **kwargs,
    )

    # if we're distincting, do it first
    if sql.distinct:
        reader = reader.distinct()

    # if the query is COUNT(*) on a SELECT, just do it.
    if str(sql.select_expression).upper() == "COUNT(*)" and not sql.group_by:
        count = -1
        for count, r in enumerate(reader):
            pass
        # we can probably safely assume a 1 record set will fit in memory
        reader = DictSet(
            iter([{"COUNT(*)": count + 1}]), storage_class=STORAGE_CLASS.MEMORY
        )

    if sql.group_by:
        from ...internals.group_by import GroupBy

        # convert the clause into something we can pass to GroupBy
        groups = [
            group.strip() for group in sql.group_by.split(",") if group.strip() != ""
        ]

        aggregations = []
        for t in sql.select_evaluator.tokens:  # type:ignore
            if t["type"] == TOKENS.AGGREGATOR:
                aggregations.append((t["value"], t["parameters"][0]["value"]))

        if aggregations:
            grouped = GroupBy(reader, *groups).aggregate(aggregations)
        else:
            grouped = GroupBy(reader, *groups).groups()
        # there could be 250000 groups, so we#re not going to load them into memory
        reader = DictSet(grouped)

        # if we have a HAVING clause, filter the grouped data by it
        if sql.having:
            reader = reader.filter(sql.having)

    if sql.order_by:
        take = 5000  # the Query UI is currently set to 2000
        if sql.limit:
            take = int(sql.limit)
        reader = DictSet(
            reader.sort_and_take(
                column=sql.order_by, take=take, descending=sql.order_descending
            )
        )
    if sql.limit:
        reader = reader.take(sql.limit)

    return reader.select(sql.select_evaluator.fields())  # type:ignore
