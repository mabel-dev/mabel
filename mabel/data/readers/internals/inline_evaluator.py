# no-maintain-checks
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
from functools import lru_cache
import re
import fastnumbers
from .inline_functions import FUNCTIONS
from ....utils.dates import parse_iso
from ....utils.token_labeler import TOKENS, get_token_type


class InvalidEvaluator(Exception):
    """custom error"""

    pass


def get_function_name(token):
    """
    Convert tokens back to a function name for inclusion in results
    """

    def _inner(tokens):
        ret = []
        for token in tokens:
            if token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):
                ret.append(get_function_name(token))
            else:
                ret.append(token["value"])
        return ret

    if token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):
        params = ",".join(_inner(token["parameters"]))
        return f"{token['value']}({params})"


def get_fields(tokens):
    def inner(tokens):
        for token in tokens:
            if token["type"] in (TOKENS.EVERYTHING):
                yield "*"
            elif token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):
                if token["as"]:
                    yield token["as"]
                else:
                    yield get_function_name(token)
            elif token["type"] in (
                TOKENS.VARIABLE,
                TOKENS.INTEGER,
                TOKENS.LITERAL,
                TOKENS.DATE,
                TOKENS.FLOAT,
                TOKENS.DATE,
            ):
                yield token["value"]

    return list(inner(tokens))


def build(tokens):
    response = []
    if not isinstance(tokens, TokenSet):
        ts = TokenSet(tokens)
    else:
        ts = tokens
    while not ts.finished():
        token = ts.token()
        ts.step()  # move along
        if token["type"] in (TOKENS.FUNCTION, TOKENS.AGGREGATOR):

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
                    if open_parentheses != 0:
                        collector.append(ts.token())
                elif ts.token()["type"] == TOKENS.LEFTPARENTHESES:
                    open_parentheses += 1
                    collector.append(ts.token())
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


def evaluate_field(dict, token):
    """
    Evaluate a single field
    """
    if token["type"] == TOKENS.EVERYTHING:
        return (TOKENS.EVERYTHING, TOKENS.EVERYTHING)
    if token["type"] == TOKENS.VARIABLE:
        return (
            token["value"],
            dict.get(token["value"]),
        )
    if token["type"] == TOKENS.FUNCTION:
        if not token["as"]:
            label = get_fields([token]).pop()
            token["as"] = label
        else:
            label = token["as"]
        return (
            label,
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
        build_phase_1 = []
        for field in self.tokens:
            build_phase_1.append(evaluate_field(dict, field))
        build_phase_2 = {k: v for k, v in build_phase_1 if k != TOKENS.EVERYTHING}
        if (TOKENS.EVERYTHING, TOKENS.EVERYTHING) in build_phase_1:
            build_phase_2.update(dict)
        return build_phase_2

    def __iter__(self):
        return self

    def __next__(self):
        if not self._iter:
            self._iter = iter(self.tokens)
        return next(self._iter)

    def fields(self):
        return get_fields(self.tokens)
