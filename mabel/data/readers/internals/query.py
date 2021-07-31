from enum import Enum
from ....utils.text import like
from ....errors import BaseException


class UnableToParseExpression(BaseException):
    pass

class TokenType(str, Enum):
    NUM = "$NUM$"
    STR = "$STR$"
    VAR = "$VAR$"
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "=="
    NEQ = "!="
    LP = "("
    RP = ")"
    AND = "AND"
    OR = "OR"
    LIKE = "LIKE"


import operator

# fmt:off
Operators = {
    TokenType.GT: { "func": operator.gt, "symbol": TokenType.GT },
    TokenType.GTE: { "func": operator.ge, "symbol": TokenType.GTE },
    TokenType.LT: { "func": operator.lt, "symbol": TokenType.LT },
    TokenType.LTE: { "func": operator.le, "symbol": TokenType.LTE },
    TokenType.EQ: { "func": operator.eq, "symbol": TokenType.EQ },
    TokenType.NEQ: { "func": operator.ne, "symbol": TokenType.NEQ },
    TokenType.LIKE: { "func": like, "symbol": TokenType.LIKE },
    TokenType.AND: { "func": operator.and_, "symbol": TokenType.AND },
    TokenType.OR: { "func": operator.or_, "symbol": TokenType.OR }
}
# fmt:on


class TreeNode(object):
    tokenType = None
    value = None
    left = None
    right = None

    def __init__(self, tokenType):
        self.tokenType = tokenType


class Tokenizer(object):
    expression = None
    tokens = None
    tokenTypes = None
    i = 0

    def __init__(self, exp):
        self.expression = exp

    def next(self):
        self.i += 1
        return self.tokens[self.i - 1]

    def peek(self):
        return self.tokens[self.i]

    def hasNext(self):
        return self.i < len(self.tokens)

    def nextTokenType(self):
        return self.tokenTypes[self.i]

    def nextTokenTypeIsOperator(self):
        t = self.tokenTypes[self.i]
        return isinstance(t, dict)

    def tokenize(self):
        import re

        reg = re.compile(r"(\bAND\b|\bOR\b|\bIN\b|\bLIKE\b|!=|==|<=|>=|<|>|\(|\))")
        self.tokens = reg.split(self.expression)
        self.tokens = [t.strip() for t in self.tokens if t.strip() != ""]

        self.tokenTypes = []
        for t in self.tokens:
            if t in Operators:
                self.tokenTypes.append(Operators[t])
            else:
                # number of string or variable
                if t[0] == t[-1] == '"' or t[0] == t[-1] == "'":
                    self.tokenTypes.append(TokenType.STR)
                else:
                    try:
                        number = float(t)
                        self.tokenTypes.append(TokenType.NUM)
                    except:
                        if re.search("^[a-zA-Z_]+$", t):
                            self.tokenTypes.append(TokenType.VAR)
                        else:
                            self.tokenTypes.append(None)

class Query(object):
    tokenizer = None
    root = None

    def __init__(self, exp):
        self.tokenizer = Tokenizer(exp)
        self.tokenizer.tokenize()
        self.parse()

    def parse(self):
        self.root = self.parseExpression()

    def parseExpression(self):
        andTerm1 = self.parseAndTerm()
        while (
            self.tokenizer.hasNext() and self.tokenizer.nextTokenType() == TokenType.OR
        ):
            self.tokenizer.next()
            andTermX = self.parseAndTerm()
            andTerm = TreeNode(TokenType.OR)
            andTerm.left = andTerm1
            andTerm.right = andTermX
            andTerm1 = andTerm
        return andTerm1

    def parseAndTerm(self):
        condition1 = self.parseCondition()
        while (
            self.tokenizer.hasNext() and self.tokenizer.nextTokenType() == TokenType.AND
        ):
            self.tokenizer.next()
            conditionX = self.parseCondition()
            condition = TreeNode(TokenType.AND)
            condition.left = condition1
            condition.right = conditionX
            condition1 = condition
        return condition1

    def parseCondition(self):
        if self.tokenizer.hasNext() and self.tokenizer.nextTokenType() == TokenType.LP:
            self.tokenizer.next()
            expression = self.parseExpression()
            if (
                self.tokenizer.hasNext()
                and self.tokenizer.nextTokenType() == TokenType.RP
            ):
                self.tokenizer.next()
                return expression
            else:
                raise UnableToParseExpression(
                    "Closing ) expected, but got " + self.tokenizer.next()
                )

        terminal1 = self.parseTerminal()
        if self.tokenizer.hasNext():
            if self.tokenizer.nextTokenTypeIsOperator():
                condition = TreeNode(self.tokenizer.nextTokenType())
                self.tokenizer.next()
                terminal2 = self.parseTerminal()
                condition.left = terminal1
                condition.right = terminal2
                return condition
            else:
                raise UnableToParseExpression(
                    "Operator expected, but got " + self.tokenizer.next()
                )
        else:
            raise UnableToParseExpression("Operator expected, but got nothing")

    def parseTerminal(self):
        if self.tokenizer.hasNext():
            tokenType = self.tokenizer.nextTokenType()
            if tokenType == TokenType.NUM:
                n = TreeNode(tokenType)
                n.value = float(self.tokenizer.next())
                return n
            elif tokenType == TokenType.VAR:
                n = TreeNode(tokenType)
                n.value = self.tokenizer.next()
                return n
            elif tokenType == TokenType.STR:
                n = TreeNode(tokenType)
                n.value = self.tokenizer.next()[1:-1]
                return n
            else:
                # NUM, STR, or VAR
                raise UnableToParseExpression(
                    "Number, Literal, or Field expected, but got "
                    + self.tokenizer.next()
                )

        else:
            # NUM, STR, or VAR
            raise UnableToParseExpression(
                "Number, Literal, or Field  expected, but got " + self.tokenizer.next()
            )

    def evaluate(self, variable_dict):
        return self.evaluateRecursive(self.root, variable_dict)

    def evaluateRecursive(self, treeNode, variable_dict):
        if treeNode.tokenType in (TokenType.NUM, TokenType.STR):
            return treeNode.value
        if treeNode.tokenType == TokenType.VAR:
            if treeNode.value in variable_dict:
                return variable_dict[treeNode.value]
            return None

        left = self.evaluateRecursive(treeNode.left, variable_dict)
        right = self.evaluateRecursive(treeNode.right, variable_dict)

        func = treeNode.tokenType.get("func")

        if func:
            return func(left, right)
        raise UnableToParseExpression("Unexpected type " + str(treeNode.tokenType))

    def indexable_predicates(self):
        return self.indexable_predicates_recursive(self.root)

    def indexable_predicates_recursive(self, treeNode):
        if treeNode.tokenType in (TokenType.NUM, TokenType.STR):
            return f"value:{treeNode.value}"
        if treeNode.tokenType == TokenType.VAR:
            return f"field:{treeNode.value}"

        left = self.indexable_predicates_recursive(treeNode.left)
        right = self.indexable_predicates_recursive(treeNode.right)

        if treeNode.tokenType.get("symbol") == "==":
            print (left, "==", right) 
