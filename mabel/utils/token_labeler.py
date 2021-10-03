"""
There are multiple usecases where we need to step over a set of tokens and apply
a label to them. The TokenLabeler doesn't actually apply labels, it's a helper
so this a) only needs to be maintained in one place b) behaves consistently.
"""

from functools import lru_cache
import re
import operator
import fastnumbers
from enum import Enum
from typing import Iterable, Any
from .text import like, not_like
from .dates import parse_iso
from ..data.readers.internals.inline_functions import FUNCTIONS
from ..data.internals.group_by import AGGREGATORS


def function_in(x, y):
    return x in y


OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "=": operator.eq,
    "!=": operator.ne,
    "<>": operator.ne,
    "IS": operator.is_,
    "IS NOT": operator.is_not,
    "LIKE": like,
    "NOT LIKE": not_like,
    "IN": function_in,
}


class TOKENS(str, Enum):
    INTEGER = "<Integer>"
    FLOAT = "<Float>"
    LITERAL = "<Literal>"
    VARIABLE = "<Variable>"
    BOOLEAN = "<Boolean>"
    DATE = "<Date>"
    NULL = "<Null>"
    LEFTPARENTHESES = "<LeftParentheses>"
    RIGHTPARENTHESES = "<RightParentheses>"
    COMMA = "<Comma>"
    FUNCTION = "<Function>"
    AGGREGATOR = "<Aggregator>"
    AS = "<As>"
    UNKNOWN = "<?>"
    EVERYTHING = "<*>"
    OPERATOR = "<Operator>"
    AND = "<And>"
    OR = "<Or>"
    NOT = "<Not>"


def get_token_type(token):
    """
    Guess the token type.
    """
    token = str(token)
    token_upper = token.upper()
    if token[0] == token[-1] == "`":
        # tokens in ` quotes are variables, this is how we supersede all other
        # checks, e.g. if it looks like a number but is a variable.
        return TOKENS.VARIABLE
    if token == "*":  # nosec - not a password
        return TOKENS.EVERYTHING
    if token_upper in FUNCTIONS:
        return TOKENS.FUNCTION
    if token_upper in OPERATORS:
        return TOKENS.OPERATOR
    if token_upper in AGGREGATORS:
        return TOKENS.AGGREGATOR
    if token[0] == token[-1] == '"' or token[0] == token[-1] == "'":
        # tokens in quotes are either dates or string literals, if we can
        # parse to a date, it's a date
        if parse_iso(token[1:-1]):
            return TOKENS.DATE
        else:
            return TOKENS.LITERAL
    if fastnumbers.isint(token):
        return TOKENS.INTEGER
    if fastnumbers.isfloat(token):
        return TOKENS.FLOAT
    if token in ("(", "["):
        return TOKENS.LEFTPARENTHESES
    if token in (")", "]"):
        return TOKENS.RIGHTPARENTHESES
    if re.search(r"^[^\d\W][\w\-\.]*", token):
        if token_upper in ("TRUE", "FALSE"):
            # 'true' and 'false' without quotes are booleans
            return TOKENS.BOOLEAN
        if token_upper in ("NULL", "NONE"):
            # 'null' or 'none' without quotes are nulls
            return TOKENS.NULL
        if token_upper == "AND":
            return TOKENS.AND
        if token_upper == "OR":
            return TOKENS.OR
        if token_upper == "NOT":
            return TOKENS.NOT
        if token_upper == "AS":
            return TOKENS.AS
        # tokens starting with a letter, is made up of letters, numbers,
        # hyphens, underscores and dots are probably variables. We do this
        # last so we don't miss assign other items to be a variable
        return TOKENS.VARIABLE
    # at this point, we don't know what it is
    return TOKENS.UNKNOWN


@lru_cache(1)
def build_splitter():
    # build the regex by building a list of all of the keywords
    keywords = []
    for item in FUNCTIONS:
        keywords.append(r"\b" + item + r"\b")
    for item in AGGREGATORS:
        keywords.append(r"\b" + item + r"\b")
    for item in OPERATORS:
        if item.isalpha():
            keywords.append(r"\b" + item + r"\b")
        else:
            keywords.append(item)
    for item in [
        "AND",
        "OR",
        "NOT",
        "SELECT",
        "FROM",
        "WHERE",
        "LIMIT",
        "GROUP\sBY",
        "ORDER BY",
        "DISTINCT",
        "ASC",
        "DESC",
    ]:
        keywords.append(r"\b" + item + r"\b")
    for item in ["(", ")", "[", "]", ",", "*"]:
        keywords.append(item)
    splitter = re.compile(
        r"(" + r"|".join(keywords) + r")",
        re.IGNORECASE,
    )
    return splitter


class Tokenizer:
    expression = None
    tokens = None
    token_types = None
    i = 0

    def __init__(self, exp):
        self.expression = exp

    def next(self):
        self.i += 1
        return self.tokens[self.i - 1]

    def peek(self):
        return self.tokens[self.i]

    def has_next(self):
        return self.i < len(self.tokens)

    def next_token_type(self):
        return self.token_types[self.i]

    def next_token_value(self):
        return self.tokens[self.i]

    def tokenize(self):
        self.tokens = build_splitter().split(self.expression)
        self.tokens = [t.strip() for t in self.tokens if t.strip() != ""]
        self.token_types = []
        for token in self.tokens:
            self.token_types.append(get_token_type(token))
