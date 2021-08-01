
from ....utils.text import like, tokenize
import operator

TOKENS = {
    "NUM" : "$Number$",
    "STR" : "$Literal$",
    "VAR" : "$Variable$",
    "BOOL": "$Boolean$",
    ">" : ">",
    ">=" : ">=",
    "<" : "<",
    "<=" : "<=",
    "==" : "==",
    "!=" : "!=",
    "LP" : "(",
    "RP" : ")",
    "AND" : "AND",
    "OR" : "OR",
    "LIKE" : "LIKE"
}

TOKEN_OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    "LIKE": like
}

class TreeNode:
    tokenType = None
    value = None
    left = None
    right = None

    def __init__(self, tokenType):
        self.tokenType = tokenType


class Tokenizer:
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
        return t in TOKEN_OPERATORS

    def tokenize(self):
        import re

        reg = re.compile(r"(\bAND\b|\bOR\b|!=|==|<=|>=|<|>|\(|\)|LIKE)")
        self.tokens = reg.split(self.expression)
        self.tokens = [t.strip() for t in self.tokens if t.strip() != ""]

        self.tokenTypes = []
        for t in self.tokens:
            if t in TOKENS:
                self.tokenTypes.append(t)
            else:
                if t in ('True', 'False'):
                    self.tokenTypes.append(TOKENS["BOOL"])
                elif t[0] == t[-1] == '"' or t[0] == t[-1] == "'":
                    self.tokenTypes.append(TOKENS["STR"])
                else:
                    try:
                        number = float(t)
                        self.tokenTypes.append(TOKENS["NUM"])
                    except:
                        if re.search("^[a-zA-Z_]+$", t):
                            self.tokenTypes.append(TOKENS["VAR"])
                        else:
                            self.tokenTypes.append(None)


class Query:
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
                self.tokenizer.hasNext() and self.tokenizer.nextTokenType() == TOKENS["OR"]
        ):
            self.tokenizer.next()
            andTermX = self.parseAndTerm()
            andTerm = TreeNode(TOKENS["OR"])
            andTerm.left = andTerm1
            andTerm.right = andTermX
            andTerm1 = andTerm
        return andTerm1

    def parseAndTerm(self):
        condition1 = self.parseCondition()
        while (
                self.tokenizer.hasNext() and self.tokenizer.nextTokenType() == TOKENS["AND"]
        ):
            self.tokenizer.next()
            conditionX = self.parseCondition()
            condition = TreeNode(TOKENS["AND"])
            condition.left = condition1
            condition.right = conditionX
            condition1 = condition
        return condition1

    def parseCondition(self):
        if self.tokenizer.hasNext() and self.tokenizer.nextTokenType() == TOKENS["LP"]:
            self.tokenizer.next()
            expression = self.parseExpression()
            if (
                    self.tokenizer.hasNext()
                    and self.tokenizer.nextTokenType() == TOKENS["RP"]
            ):
                self.tokenizer.next()
                return expression
            else:
                raise Exception("Closing ) expected, but got " + self.tokenizer.next())

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
                raise Exception("Operator expected, but got " + self.tokenizer.next())
        else:
            raise Exception("Operator expected, but got nothing")

    def parseTerminal(self):
        if self.tokenizer.hasNext():
            tokenType = self.tokenizer.nextTokenType()
            if tokenType == TOKENS["NUM"]:
                n = TreeNode(tokenType)
                n.value = float(self.tokenizer.next())
                return n
            elif tokenType == TOKENS["VAR"]:
                n = TreeNode(tokenType)
                n.value = self.tokenizer.next()
                return n
            elif tokenType == TOKENS["STR"]:
                n = TreeNode(tokenType)
                n.value = self.tokenizer.next()[1:-1]
                return n
            elif tokenType == TOKENS["BOOL"]:
                n = TreeNode(tokenType)
                n.value = self.tokenizer.next() == 'True'
                return n
            else:
                raise Exception(
                    "NUM, STR, or VAR expected, but got " + self.tokenizer.next()
                )

        else:
            raise Exception(
                "NUM, STR, or VAR expected, but got " + self.tokenizer.next()
            )

    def evaluate(self, variable_dict):
        return self.evaluateRecursive(self.root, variable_dict)

    def evaluateRecursive(self, treeNode, variable_dict):
        if treeNode.tokenType in (TOKENS["NUM"], TOKENS["STR"], TOKENS["BOOL"]):
            return treeNode.value
        if treeNode.tokenType == TOKENS["VAR"]:
            if treeNode.value in variable_dict:
                value = variable_dict[treeNode.value]
                if isinstance(value, int):
                    return float(value)
                if value in ('True', 'False'):
                    return value == 'True'
                return value 
            return None

        left = self.evaluateRecursive(treeNode.left, variable_dict)
        right = self.evaluateRecursive(treeNode.right, variable_dict)
        if treeNode.tokenType in TOKEN_OPERATORS:
            return TOKEN_OPERATORS[treeNode.tokenType](left, right)
        elif treeNode.tokenType == TOKENS["AND"]:
            return left and right
        elif treeNode.tokenType == TOKENS["OR"]:
            return left or right
        else:
            raise Exception("Unexpected type " + str(treeNode.tokenType))


    def to_dnf(self):
        return self.inner_to_dnf(self.root)

    def inner_to_dnf(self, treeNode):
        if treeNode.tokenType in (TOKENS["NUM"], TOKENS["STR"], TOKENS["VAR"]):
            return treeNode.value

        left = self.inner_to_dnf(treeNode.left)
        right = self.inner_to_dnf(treeNode.right)

        if treeNode.tokenType == TOKENS["AND"]:
            return [left, right]

        if treeNode.tokenType == TOKENS["OR"]:
            return [left],[right]

        if treeNode.tokenType in TOKEN_OPERATORS:
            return (left, treeNode.tokenType, right)

