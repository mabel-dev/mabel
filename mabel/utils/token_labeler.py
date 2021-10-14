# no-maintain-checks
"""
There are multiple usecases where we need to step over a set of tokens and apply
a label to them. Doing this in a helper module means a) it only needs to be maintained
in one place b) different parts of the system behave consistently.
"""

from functools import lru_cache
import re
import operator
import fastnumbers
from enum import Enum
from .text import like, not_like
from .dates import parse_iso
from ..data.readers.internals.inline_functions import FUNCTIONS
from ..data.internals.group_by import AGGREGATORS

# These are the characters we should escape in our regex
REGEX_CHARACTERS = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


class TokenError(Exception):
    pass


def function_in(x, y):
    return x in y


def function_contains(x, y):
    return y in x


# the order of the operators affects the regex, e.g. <> needs to be defined before
# < otherwise that will be matched and the > will be invalid syntax.
OPERATORS = {
    "<>": operator.ne,
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
    "=": operator.eq,
    "IS NOT": operator.is_not,
    "IS": operator.is_,
    "NOT LIKE": not_like,
    "LIKE": like,
    "IN": function_in,
    "CONTAINS": function_contains,
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
    SUBQUERY = "<SubQuery>"
    EMPTY = "<Empty>"


def get_token_type(token):
    """
    Guess the token type.
    """
    token = str(token).strip()
    token_upper = token.upper()
    if len(token) == 0:
        return TOKENS.EMPTY
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
        if token_upper == "AND":  # nosec - not a password
            return TOKENS.AND
        if token_upper == "OR":  # nosec - not a password
            return TOKENS.OR
        if token_upper == "NOT":  # nosec - not a password
            return TOKENS.NOT
        if token_upper == "AS":  # nosec - not a password
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
        if item.replace(" ", "").isalpha():
            keywords.append(r"\b" + item + r"\b")
        else:
            keywords.append("".join([REGEX_CHARACTERS.get(ch, ch) for ch in item]))
    for item in [
        "AND",
        "OR",
        "NOT",
        "SELECT",
        "FROM",
        "WHERE",
        "LIMIT",
        r"GROUP\sBY",
        r"ORDER\sBY",
        "DISTINCT",
        "ASC",
        "DESC",
        "IN",
    ]:
        keywords.append(r"\b" + item + r"\b")
    for item in ["(", ")", "[", "]", ",", "*"]:
        keywords.append("".join([REGEX_CHARACTERS.get(ch, ch) for ch in item]))
    splitter = re.compile(
        r"(" + r"|".join(keywords) + r")",
        re.IGNORECASE,
    )
    return splitter


class Tokenizer:

    slots = ("i", "tokens")

    def __init__(self, exp):
        self.i = 0
        if isinstance(exp, str):
            self.tokens = self.tokenize(exp)
        else:
            self.tokens = exp

    def next(self):
        self.i += 1
        return self.tokens[self.i - 1]

    def peek(self):
        return self.tokens[self.i]

    def has_next(self):
        return self.i < len(self.tokens)

    def next_token_type(self):
        return get_token_type(self.tokens[self.i])

    def next_token_value(self):
        return self.tokens[self.i]

    def _fix_special_chars(self, tokens):

        builder = ""
        looking_for_end_char = None

        for token in tokens:
            stripped_token = token.strip()
            if len(stripped_token) == 0:
                # the splitter creates unwanted empty strings
                pass

            elif not looking_for_end_char and stripped_token[0] not in ('"', "'", "`"):
                # nothing interesting here
                yield token

            elif (
                stripped_token[0] in ('"', "'", "`")
                and stripped_token[-1] == stripped_token[0]
                and len(stripped_token) > 1
            ):
                # the quotes wrap the entire token
                yield token

            elif stripped_token[-1] == looking_for_end_char:
                # we've found the end of the token, yield it and reset
                builder += token
                yield builder
                builder = ""
                looking_for_end_char = None

            elif stripped_token[0] in ('"', "'", "`") and (
                stripped_token[-1] != stripped_token[0] or len(stripped_token) == 1
            ):
                # we've found a new token to collect
                # the last character will always equal the last character if there's only one
                builder = token
                looking_for_end_char = stripped_token[0]

            elif looking_for_end_char:
                # we're building a token
                builder += token

            else:
                raise TokenError(
                    "Unable to determine quoted token boundaries, you may be missing a closing quote."
                )

    def clean_statement(self, string):
        """
        Remove carriage returns and all whitespace to single spaces
        """
        whitespace_cleaner = re.compile(r"\s+")
        return whitespace_cleaner.sub(" ", string).strip()

    def tokenize(self, expression):
        expression = self.clean_statement(expression)
        tokens = build_splitter().split(expression)
        # characters like '*' in literals break the tokenizer, so we need to fix them
        tokens = list(self._fix_special_chars(tokens))
        tokens = [t.strip() for t in tokens if t.strip() != ""]
        return tokens

    def __str__(self):
        return self.peek()
