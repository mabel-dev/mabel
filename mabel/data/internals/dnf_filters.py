"""
This is a filtering mechanism to be applied when reading data.
"""

import operator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from mabel.errors import InvalidSyntaxError
from mabel.utils.text import like
from mabel.utils.text import matches

# convert text representation of operators to functions
OPERATORS = {
    "=": operator.eq,
    "==": operator.eq,
    "is": operator.eq,
    "!=": operator.ne,
    "<>": operator.ne,
    "<": operator.lt,
    ">": operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "like": like,
    "matches": matches,
    "~": matches,
    "in": lambda x, y: x in y,
    "!in": lambda x, y: x not in y,
    "not in": lambda x, y: x not in y,
    "contains": lambda x, y: y in x,
    "!contains": lambda x, y: y not in x,
}


def evaluate(predicate: Union[tuple, list], record: dict) -> bool:
    """
    This is the evaluation routine for the Filter class.

    Implements a DNF (Disjunctive Normal Form) interpretter. Predicates in the same
    list are joined with an AND (*all()*) and predicates in adjacent lists are joined
    with an OR (*any()*). This allows for non-trivial filters to be written with just
    _tuples_ and _lists_.

    The predicates are in _tuples_ in the form (`key`, `op`, `value`) where the `key`
    is the value looked up from the record, the `op` is the operator and the `value`
    is a literal.
    """
    # No filter doesn't filter
    if predicate is None:  # pragma: no cover
        return True

    # If we have a tuple extract out the key, operator and value and do the evaluation
    if isinstance(predicate, tuple):
        key, op, value = predicate
        record_value = record.get(key, None)
        return record_value is not None and OPERATORS[op.lower()](record_value, value)

    if isinstance(predicate, list):
        # Are all of the entries tuples?
        # We AND them together (_all_ are True)
        if all(isinstance(p, tuple) for p in predicate):
            return all(evaluate(p, record) for p in predicate)

        # Are all of the entries lists?
        # We OR them together (_any_ are True)
        if all(isinstance(p, list) for p in predicate):
            return any(evaluate(p, record) for p in predicate)

        # if we're here the structure of the filter is wrong
        raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

    raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover


class DnfFilters:
    __slots__ = ("empty_filter", "predicates")

    def __init__(self, filters: Optional[List[Tuple[str, str, object]]] = None):
        """
        A class to supporting filtering data.

        Parameters:
            filters: list of tuples
                Each tuple has format: (`key`, `op`, `value`). When run the
                filter will extract the `key` field from the dictionary and
                compare to the `value` using the operator `op`. Multiple
                filters are treated as AND, lists of ANDs are treated as ORs.
                The supported `op` values are: `=` or `==`, `!=`, `<`, `>`,
                `<=`, `>=`, `in`, `!in` (not in), `contains`, `!contains`
                (does not contain) and `like`. If the `op` is `in` or `!in`,
                the `value` must be a collection such as a _list_, a _set_
                or a _tuple_.
                `like` performs similar to the SQL operator, `%` is a
                multi-character wildcard and `_` is a single character
                wildcard.

        Examples:
            filters = Filters([('name', '==', 'john')])
            filters = Filters([('name', '!=', 'john'),('name', '!=', 'tom')])

        """
        self.empty_filter = filters is None
        self.predicates = filters if filters else []

    def filter_dictset(self, dictset: Iterable[dict]) -> Iterable:
        """
        Tests each entry in a Iterable against the filters

        Parameters:
            dictset: iterable of dictionaries
                The dictset to process

        Yields:
            dictionary
        """
        if self.empty_filter:
            yield from dictset
        else:
            yield from (record for record in dictset if evaluate(self.predicates, record))

    def __call__(self, record) -> bool:
        return evaluate(self.predicates, record)
