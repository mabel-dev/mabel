"""
There are multiple usecases where we need to step over a set of tokens and apply
a label to them. The TokenLabeler doesn't actually apply labels, it's a helper
so this a) only needs to be maintained in one place b) behaves consistently.
"""
import re
import operator
import fastnumbers
from enum import Enum
from typing import Iterable, Any
from .text import like, not_like
from .dates import parse_iso
from ..data.readers.internals.inline_functions import FUNCTIONS


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
    "IN": function_in
    ## BETWEEN
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
    if token[0] == token[-1] == "`":
        # tokens in ` quotes are variables, this is how we supersede all other
        # checks, e.g. if it looks like a number but is a variable.
        return TOKENS.VARIABLE
    if token == "*":  # nosec - not a password
        return TOKENS.EVERYTHING
    if token.upper() in FUNCTIONS:
        # if it matches a function call, it is
        return TOKENS.FUNCTION
    if token.upper() in OPERATORS:
        # if it matches an operator, it is
        return TOKENS.OPERATOR
    if token.lower() in ("true", "false"):
        # 'true' and 'false' without quotes are booleans
        return TOKENS.BOOLEAN
    if token.lower() in ("null", "none"):
        # 'null' or 'none' without quotes are nulls
        return TOKENS.NULL
    if token.lower() == "and":
        return TOKENS.AND
    if token.lower() == "or":
        return TOKENS.OR
    if token.lower() == "not":
        return TOKENS.NOT
    if token[0] == token[-1] == '"' or token[0] == token[-1] == "'":
        # tokens in quotes are either dates or string literals
        if parse_iso(token[1:-1]):
            return TOKENS.DATE
        else:
            return TOKENS.LITERAL
    if fastnumbers.isint(token):
        # if we can parse to an int, it's an int
        return TOKENS.INTEGER
    if fastnumbers.isfloat(token):
        # if we can parse to a float, it's a float
        return TOKENS.FLOAT
    if token.upper() == "AS":
        # AS looks like a variable but us a keyword
        return TOKENS.AS
    if re.search(r"^[^\d\W][\w\-\.]*", token):
        # tokens starting with a letter, is made up of letters, numbers,
        # hyphens, underscores and dots are probably variables
        return TOKENS.VARIABLE
    if token in ("(", "["):
        return TOKENS.LEFTPARENTHESES
    if token in (")", "]"):
        return TOKENS.RIGHTPARENTHESES
    # at this point, we don't know what it is
    return TOKENS.UNKNOWN


class TokenLabeler:
    def __init__(self, tokens: Iterable):
        self.index = 0
        self.token_count = len(tokens)
        self.tokens = tokens

    def fin(self) -> bool:
        """
        Are we at the end of the Tokens
        """
        return self.index >= self.token_count

    def peek(self) -> Any:
        """
        What is the next Token, without changing the focus
        """
        if (self.index + 1) < self.token_count:
            return self.tokens[self.index + 1]

    def item(self) -> Any:
        """
        What is the current Token
        """
        return self.tokens[self.index]

    def step(self):
        """
        Move to the next token
        """
        self.index += 1
        return self.index
