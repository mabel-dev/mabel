"""
There are multiple usecases where we need to step over a set of tokens and apply
a label to them. The TokenLabeler doesn't actually apply labels - it's a helper.
"""

from typing import Iterable, Any


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
        if not self.fin():
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
