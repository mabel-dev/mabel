# no-maintain-checks
"""
This implements a SQL-like query syntax to filter dictionaries based on combinations
of predicates.

The implementation is sort of an expression tree, it doesn't need to be a complete
expression tree as it's only doing boolean logic.

Derived from: https://gist.github.com/leehsueh/1290686
"""
import re
import fastnumbers
from ..readers.internals.inline_evaluator import *
from ...utils.dates import parse_iso
from ...utils.token_labeler import get_token_type, TOKENS, OPERATORS


class InvalidExpression(BaseException):
    pass



SPLITTER = re.compile(
    r"(\bNONE\b|\bNULL\b|\bIS\sNOT\b|\bIS\b|\bAND\b|\bOR\b|!=|==|=|\<\>|<=|>=|<|>|\(|\)|\bNOT\sLIKE\b|\bLIKE\b|\bNOT\b)",
    re.IGNORECASE,
)


class TreeNode:
    __slots__ = ("token_type", "value", "left", "right")

    def __init__(self, token_type, value):
        self.token_type = token_type
        self.value = value
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

    def next_token_value(self):
        return self.tokens[self.i]

    def tokenize(self):
        self.tokens = SPLITTER.split(self.expression)
        self.tokens = [t.strip() for t in self.tokens if t.strip() != ""]

        self.token_types = []
        for token in self.tokens:

            token = str(token)
            self.token_types.append(get_token_type(token))


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
            and self.tokenizer.next_token_type() == TOKENS.OR
        ):
            self.tokenizer.next()
            andTermX = self.parse_and_term()
            andTerm = TreeNode(TOKENS.OR, None)
            andTerm.left = andTerm1
            andTerm.right = andTermX
            andTerm1 = andTerm
        return andTerm1

    def parse_and_term(self):
        condition1 = self.parse_condition()
        while (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS.AND
        ):
            self.tokenizer.next()
            conditionX = self.parse_condition()
            condition = TreeNode(TOKENS.AND, None)
            condition.left = condition1
            condition.right = conditionX
            condition1 = condition
        return condition1

    def parse_condition(self):

        terminal1 = None

        if (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS.NOT
        ):
            not_condition = TreeNode(self.tokenizer.next_token_type(), None)
            self.tokenizer.next()
            child_condition = self.parse_condition()
            not_condition.left = child_condition
            not_condition.right = None  # NOT is a unary operator
            return not_condition
    
        if self.tokenizer.has_next() and self.tokenizer.next_token_type() == TOKENS.FUNCTION:
            # we don't evaluate functions as part of this module, the function call
            # should be part of the record, e.g. record["YEAR(date_of_birth)"]
            # we we extract out the function and treat is as a VARIABLE

            collector = [self.tokenizer.next_token_value(), "("]

            self.tokenizer.next()
            if not self.tokenizer.has_next or self.tokenizer.next_token_type() != TOKENS.LEFTPARENTHESES:
                raise InvalidExpression("Functions should be followed by paranthesis")

            self.tokenizer.next()
            open_parentheses = 1

            while open_parentheses > 0:
                if not self.tokenizer.has_next():
                    break
                if self.tokenizer.next_token_type() == TOKENS.RIGHTPARENTHESES:
                    open_parentheses -= 1
                elif self.tokenizer.next_token_type() == TOKENS.LEFTPARENTHESES:
                    open_parentheses += 1
                collector.append(self.tokenizer.next())

            if open_parentheses != 0:
                raise InvalidExpression("Unbalanced parantheses")

            terminal1 = TreeNode(TOKENS.VARIABLE, ''.join(collector))

        if (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS.LEFTPARENTHESES
        ):
            self.tokenizer.next()
            expression = self.parse_expression()
            if (
                self.tokenizer.has_next()
                and self.tokenizer.next_token_type() == TOKENS.RIGHTPARENTHESES
            ):
                self.tokenizer.next()
                return expression
            raise InvalidExpression(f"`)` expected, but got `{self.tokenizer.next()}`")

        if not terminal1:
            terminal1 = self.parse_terminal()
        if self.tokenizer.has_next():
            if self.tokenizer.next_token_type() == TOKENS.OPERATOR:
                condition = TreeNode(self.tokenizer.next_token_type(), self.tokenizer.next_token_value().upper())
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
            if token_type == TOKENS.INTEGER:
                n = TreeNode(token_type, fastnumbers.fast_int(self.tokenizer.next()))
                return n
            if token_type == TOKENS.FLOAT:
                n = TreeNode(token_type, fastnumbers.fast_float(self.tokenizer.next()))
                return n
            if token_type in (TOKENS.VARIABLE, TOKENS.NOT):
                n = TreeNode(token_type, self.tokenizer.next())
                return n
            if token_type == TOKENS.LITERAL:
                n = TreeNode(token_type, self.tokenizer.next()[1:-1])
                return n
            if token_type == TOKENS.BOOLEAN:
                n = TreeNode(token_type, self.tokenizer.next().lower() == "true")
                return n
            if token_type == TOKENS.NULL:
                n = TreeNode(token_type, None)
                return n
            if token_type == TOKENS.DATE:
                n = TreeNode(token_type, parse_iso(self.tokenizer.next()[1:-1]))
                return n

        raise InvalidExpression(f"Unexpected token, got `{self.tokenizer.next()}`")

    def evaluate(self, variable_dict):
        return self.evaluate_recursive(self.root, variable_dict)

    def __call__(self, variable_dict):
        return self.evaluate_recursive(self.root, variable_dict)

    def evaluate_recursive(self, treeNode, variable_dict):
        if treeNode.token_type in (
            TOKENS.INTEGER,
            TOKENS.FLOAT,
            TOKENS.LITERAL,
            TOKENS.BOOLEAN,
            TOKENS.NULL,
            TOKENS.DATE,
        ):
            return treeNode.value
        if treeNode.token_type == TOKENS.VARIABLE:
            if treeNode.value in variable_dict:
                value = variable_dict[treeNode.value]
                if str(value).isdecimal() or str(value).isnumeric():
                    return fastnumbers.fast_float(value)
                if isinstance(value, str) and parse_iso(value):
                    return parse_iso(value)
                if value in ("True", "False"):
                    return value.lower() == "true"
                return value
            return None

        left = self.evaluate_recursive(treeNode.left, variable_dict)

        if treeNode.token_type == TOKENS.NOT:
            return not left

        right = self.evaluate_recursive(treeNode.right, variable_dict)

        if treeNode.token_type == TOKENS.OPERATOR:
            return OPERATORS[treeNode.value](left, right)
        if treeNode.token_type == TOKENS.AND:
            return left and right
        if treeNode.token_type == TOKENS.OR:
            return left or right

        raise InvalidExpression(
            f"Unexpected value of type `{str(treeNode.token_type)}`"
        )

    def to_dnf(self):
        """
        Converting to DNF as sometimes it's easier to deal with DNF than an
        expression tree.
        """
        return self._inner_to_dnf(self.root)

    def _inner_to_dnf(self, treeNode):
        if treeNode.token_type in (
            TOKENS.INTEGER,
            TOKENS.FLOAT,
            TOKENS.BOOLEAN,
            TOKENS.NULL,
            TOKENS.VARIABLE,
        ):
            return treeNode.value

        if treeNode.token_type in (
            TOKENS.DATE,
            TOKENS.LITERAL
        ):
            return f'"{treeNode.value}"'

        left = self._inner_to_dnf(treeNode.left)
        right = None
        if treeNode.right:
            right = self._inner_to_dnf(treeNode.right)

        if treeNode.token_type == TOKENS.AND:
            return [left, right]

        if treeNode.token_type == TOKENS.OR:
            return [[left], [right]]

        if treeNode.token_type == TOKENS.OPERATOR:
            return (left, treeNode.value, right)

        if treeNode.token_type == TOKENS.NOT:
            # this isn't strict DNF
            return ('NOT', left)
