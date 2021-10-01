"""
This class performs functions on individual rows. There is a set of functions in
the sql_functions module.

We interpret the evaluation line as a set of values, usually made up of Functions
and Variables, the functions may include constants of different types.

e.g.

Evaluator("LEFT(NAME, 1), AGE").evaluate(dic)

will perform the function LEFT on the NAME field from the dict and return AGE
from the dict
"""
import re
import fastnumbers
from enum import Enum
from .inline_functions import FUNCTIONS
from ....utils.dates import parse_iso


class InvalidEvaluator(Exception):
    """custom error"""

    pass


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


def get_token_type(token):
    """
    Guess the token type - tokens must have no spaces.
    """
    token = str(token)
    if token[0] == token[-1] == "`":
        # tokens in ` quotes are variables, this is how we supersede all other
        # checks, e.g. if it looks like a number but is a variable.
        return TOKENS.VARIABLE
    if token == "*":
        return TOKENS.EVERYTHING
    if token.upper() in FUNCTIONS:
        # if it matches a function call, it is
        return TOKENS.FUNCTION
    if token.lower() in ("true", "false"):
        # 'true' and 'false' without quotes are booleans
        return TOKENS.BOOLEAN
    if token.lower() in ("null", "none"):
        # 'null' or 'none' without quotes are nulls
        return TOKENS.NULL
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
    # if it doesn't match the above, we don't know what this is
    if token in ("(", "["):
        return TOKENS.LEFTPARENTHESES
    if token in (")", "]"):
        return TOKENS.RIGHTPARENTHESES
    return TOKENS.UNKNOWN


def build(tokens):
    response = []
    if not isinstance(tokens, TokenSet):
        ts = TokenSet(tokens)
    else:
        ts = tokens
    while not ts.finished():
        token = ts.token()
        ts.step()  # move along
        if token["type"] == TOKENS.FUNCTION:

            if not ts.token()["type"] == TOKENS.LEFTPARENTHESES:
                raise InvalidEvaluator("Invalid expression, missing expected `(` ")
            ts.step()  # step over the (

            # collect all the tokens between the parentheses and 'build' them
            open_parentheses = 1
            collector = []
            while open_parentheses > 0:
                if ts.finished():
                    break
                if ts.token()["type"] == TOKENS.RIGHTPARENTHESES:
                    open_parentheses -= 1
                elif ts.token()["type"] == TOKENS.LEFTPARENTHESES:
                    open_parentheses += 1
                else:
                    collector.append(ts.token())
                ts.step()

            if open_parentheses != 0:
                raise InvalidEvaluator("Unbalanced parantheses")

            token["parameters"] = build(collector)

        if not ts.finished() and ts.token()["type"] == TOKENS.AS:
            ts.step()
            if ts.finished():
                raise InvalidEvaluator("Incomplete statement after AS")
            token["as"] = ts.token()["value"]
            ts.step()
        response.append(token)
    return response


def if_as(token, name):
    """
    Deal with AS directives
    """
    if token.get("as") is None:
        return name
    return token["as"]


def evaluate_field(dict, token):
    """
    Evaluate a single field
    """
    if token["type"] == TOKENS.VARIABLE:
        return (
            token["value"],
            dict.get(token["value"]),
        )
    if token["type"] == TOKENS.FUNCTION:
        function_name = f"{token['value'].upper()}({','.join([t['value'] for t in token['parameters']])})"
        return (
            if_as(token, function_name),
            FUNCTIONS[token["value"].upper()](
                *[evaluate_field(dict, t)[1] for t in token["parameters"]]
            ),
        )
    if token["type"] == TOKENS.FLOAT:
        return (
            token["value"],
            fastnumbers.fast_float(token["value"]),
        )
    if token["type"] == TOKENS.INTEGER:
        return (
            token["value"],
            fastnumbers.fast_int(token["value"]),
        )
    if token["type"] == TOKENS.LITERAL:
        return (
            token["value"],
            str(token["value"])[1:-1],
        )
    if token["type"] == TOKENS.DATE:
        return (
            token["value"],
            parse_iso(token["value"][1:-1]),
        )
    if token["type"] == TOKENS.BOOLEAN:
        return (
            token["value"],
            str(token["value"]).upper() == "TRUE",
        )
    if token["type"] == TOKENS.NULL:
        return (
            token["value"],
            None,
        )
    return (
        token["value"],
        None,
    )


class TokenSet(list):
    def __init__(self, tokens):
        self._tokens = tokens
        self._index = 0
        self._max = len(tokens)

    def token(self):
        token = self._tokens[self._index]
        if isinstance(token, dict):
            return token
        return {
            "value": token,
            "type": get_token_type(token),
            "parameters": [],
            "as": None,
        }

    def step(self):
        if self._index < self._max:
            self._index += 1

    def next(self):
        self._index += 1
        ret = {"type": None}
        if self._index < self._max:
            ret = self.token()
        self._index -= 1
        return ret

    def finished(self):
        return self._index == self._max


class Evaluator:
    def __init__(self, proforma):
        reg = re.compile(r"(\(|\)|,|\bAS\b)", re.IGNORECASE)
        tokens = [t.strip() for t in reg.split(proforma) if t.strip() not in ("", ",")]
        self.tokens = build(tokens)
        self._iter = None

    def __call__(self, dict):
        ret = []
        for field in self.tokens:
            ret.append(evaluate_field(dict, field))
        return {k: v for k, v in ret}

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iter:
            self._iter = iter(self.tokens)
        return next(self._iter)
