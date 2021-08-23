# no-maintain-checks
"""
This implements a SQL-like query syntax to filter dictionaries based on combinations
of predicates.

The implementation is as an expression tree, 

Derived from: https://gist.github.com/leehsueh/1290686
"""

from ...utils.text import like, not_like
import re
import operator


class InvalidExpression(BaseException):
    pass


TOKENS = {
    "NUM": "<Number>",
    "STR": "<Literal>",
    "VAR": "<Variable>",
    "BOOL": "<Boolean>",
    "NULL": "<Null>",
    "NOT": "NOT",
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "IS": "IS",
    "IS NOT": "IS NOT",
    "=": "==",
    "==": "==",
    "!=": "!=",
    "<>": "!=",
    "LP": "(",
    "RP": ")",
    "AND": "AND",
    "OR": "OR",
    "LIKE": "LIKE",
    "NOT LIKE": "NOT LIKE",
}

TOKEN_OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    "IS": operator.is_,
    "IS NOT": operator.is_not,
    "LIKE": like,
    "NOT LIKE": not_like,
}

SPLITTER = re.compile(
    r"(\bNONE\b|\bNULL\b|\bIS\sNOT\b|\bIS\b|\bAND\b|\bOR\b|!=|==|=|\<\>|<=|>=|<|>|\(|\)|\bNOT\sLIKE\b|\bLIKE\b|\bNOT\b)",
    re.IGNORECASE,
)


class TreeNode:
    __slots__ = ("token_type", "value", "left", "right")

    def __init__(self, token_type):
        self.token_type = token_type
        self.value = None
        self.left = None
        self.right = None


class ExpressionTokenizer:
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

    def next_token_type_is_operator(self):
        t = self.token_types[self.i]
        return t in TOKEN_OPERATORS

    def tokenize(self):
        self.tokens = SPLITTER.split(self.expression)
        self.tokens = [t.strip() for t in self.tokens if t.strip() != ""]

        self.token_types = []
        for t in self.tokens:
            if t.upper() in TOKENS:
                self.token_types.append(TOKENS[t.upper()])
            else:
                if str(t).lower() in ("true", "false"):
                    self.token_types.append(TOKENS["BOOL"])
                elif str(t).lower() in ("null", "none"):
                    self.token_types.append(TOKENS["NULL"])
                elif t[0] == t[-1] == '"' or t[0] == t[-1] == "'":
                    self.token_types.append(TOKENS["STR"])
                else:
                    try:
                        number = float(t)
                        self.token_types.append(TOKENS["NUM"])
                    except:
                        # starts with a letter, is made up of letters, numbers,
                        # hyphens, underscores and dots
                        if re.search(r"^[^\d\W][\w\-\.]*", t):
                            self.token_types.append(TOKENS["VAR"])
                        else:
                            self.token_types.append(None)

class Expression(object):
    tokenizer = None
    root = None

    def __init__(self, exp):
        self.tokenizer = ExpressionTokenizer(exp)
        self.tokenizer.tokenize()
        self.parse()

    def parse(self):
        self.root = self.parse_expression()

    def parse_expression(self):
        andTerm1 = self.parse_and_term()
        while (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS["OR"]
        ):
            self.tokenizer.next()
            andTermX = self.parse_and_term()
            andTerm = TreeNode(TOKENS["OR"])
            andTerm.left = andTerm1
            andTerm.right = andTermX
            andTerm1 = andTerm
        return andTerm1

    def parse_and_term(self):
        condition1 = self.parse_condition()
        while (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS["AND"]
        ):
            self.tokenizer.next()
            conditionX = self.parse_condition()
            condition = TreeNode(TOKENS["AND"])
            condition.left = condition1
            condition.right = conditionX
            condition1 = condition
        return condition1

    def parse_condition(self):
        if (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS["NOT"]
        ):
            not_condition = TreeNode(self.tokenizer.next_token_type())
            self.tokenizer.next()
            child_condition = self.parse_condition()
            not_condition.left = child_condition
            not_condition.right = None  # NOT is a unary operator
            return not_condition
        if (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS["LP"]
        ):
            self.tokenizer.next()
            expression = self.parse_expression()
            if (
                self.tokenizer.has_next()
                and self.tokenizer.next_token_type() == TOKENS["RP"]
            ):
                self.tokenizer.next()
                return expression
            raise InvalidExpression(f"`)` expected, but got `{self.tokenizer.next()}`")

        terminal1 = self.parse_terminal()
        if self.tokenizer.has_next():
            if self.tokenizer.next_token_type_is_operator():
                condition = TreeNode(self.tokenizer.next_token_type())
                self.tokenizer.next()
                terminal2 = self.parse_terminal()
                condition.left = terminal1
                condition.right = terminal2
                return condition
            raise InvalidExpression(
                f"Operator expected, but got `{self.tokenizer.next()}`"
            )
        raise InvalidExpression("Operator expected, but got nothing")

    def parse_terminal(self):
        if self.tokenizer.has_next():
            token_type = self.tokenizer.next_token_type()
            if token_type == TOKENS["NUM"]:
                n = TreeNode(token_type)
                n.value = float(self.tokenizer.next())
                return n
            if token_type in (TOKENS["VAR"], TOKENS["NOT"]):
                n = TreeNode(token_type)
                n.value = self.tokenizer.next()
                return n
            if token_type == TOKENS["STR"]:
                n = TreeNode(token_type)
                n.value = self.tokenizer.next()[1:-1]
                return n
            if token_type == TOKENS["BOOL"]:
                n = TreeNode(token_type)
                n.value = self.tokenizer.next().lower() == "true"
                return n
            if token_type == TOKENS["NULL"]:
                n = TreeNode(token_type)
                n.value = None
                return n

        raise InvalidExpression(f"Unexpected token, got `{self.tokenizer.next()}`")

    def evaluate(self, variable_dict):
        return self.evaluate_recursive(self.root, variable_dict)

    def evaluate_recursive(self, treeNode, variable_dict):
        if treeNode.token_type in (
            TOKENS["NUM"],
            TOKENS["STR"],
            TOKENS["BOOL"],
            TOKENS["NULL"],
        ):
            return treeNode.value
        if treeNode.token_type == TOKENS["VAR"]:
            if treeNode.value in variable_dict:
                value = variable_dict[treeNode.value]
                if isinstance(value, int):
                    return float(value)
                if value in ("True", "False"):
                    return value.lower() == "true"
                return value
            return None

        left = self.evaluate_recursive(treeNode.left, variable_dict)

        if treeNode.token_type == TOKENS["NOT"]:
            return not left

        right = self.evaluate_recursive(treeNode.right, variable_dict)

        if treeNode.token_type in TOKEN_OPERATORS:
            return TOKEN_OPERATORS[treeNode.token_type](left, right)
        if treeNode.token_type == TOKENS["AND"]:
            return left and right
        if treeNode.token_type == TOKENS["OR"]:
            return left or right

        raise InvalidExpression(
            f"Unexpected value of type `{str(treeNode.token_type)}`"
        )

    def to_dnf(self):
        return self.inner_to_dnf(self.root)

    def inner_to_dnf(self, treeNode):
        if treeNode.token_type in (
            TOKENS["NUM"],
            TOKENS["STR"],
            TOKENS["VAR"],
            TOKENS["BOOL"],
            TOKENS["NULL"],
        ):
            return treeNode.value

        left = self.inner_to_dnf(treeNode.left)
        right = self.inner_to_dnf(treeNode.right)

        if treeNode.token_type == TOKENS["AND"]:
            return [left, right]

        if treeNode.token_type == TOKENS["OR"]:
            return [left], [right]

        if treeNode.token_type in TOKEN_OPERATORS:
            return (left, treeNode.token_type, right)
