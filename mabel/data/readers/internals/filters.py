"""
This is an initial implementation of a filtering mechanism to be applied
when reading data. There is an existing function-based implemented
however, as this makes it difficult to understand the fields being
used to filter on, when data is indexed it won't be able to take 
advantage of the filters.

This current implementation is a step toward that, it does not use
any indices at this point.
"""
from enum import Enum
from functools import lru_cache
from typing import Optional, Any, Iterable, List, Tuple, Union
#from ....errors import InvalidSyntaxError
import re

InvalidSyntaxError = Exception


# https://codereview.stackexchange.com/a/248421
_special_regex_chars = {
    ch : '\\'+ch
    for ch in '.^$*+?{}[]|()\\'
}

@lru_cache(4)
def _sql_like_fragment_to_regex(fragment):
    """
    Allows us to accepts LIKE statements to search data
    """
    # https://codereview.stackexchange.com/a/36864/229677
    safe_fragment = ''.join([_special_regex_chars.get(ch, ch) for ch in fragment])
    return re.compile('^' + safe_fragment.replace('%', '.*?').replace('_', '.') + '$')

# set of operators we can interpret 
def _eq(x,y):   return x == y
def _neq(x,y):  return x != y
def _lt(x,y):   return x < y
def _gt(x,y):   return x > y
def _lte(x,y):  return x <= y
def _gte(x,y):  return x >= y
def _and(x,y):  return x and y
def _or(x,y):   return x or y
def _like(x,y): return _sql_like_fragment_to_regex(y).match(str(x))
def _in(x,y):   return x in y
def _nin(x,y):  return x not in y
def true(x):    return True

# convert text representation of operators to functions
OPERATORS = {
    '='   : _eq,
    '=='  : _eq,
    '!='  : _neq,
    '<'   : _lt,
    '>'   : _gt,
    '<='  : _lte,
    '>='  : _gte,
    'and' : _and,
    'or'  : _or,
    'like': _like,
    'in'  : _in,
    '!in' : _nin
}

def evaluate(
        predicate: Union[tuple, list],
        record: dict) -> bool:
    """
    This evaluation routine for the Filter class.

    Implements a DNF (Disjunctive Normal Form) interpretter. Predicates in the
    same list are joined with an AND (_all()_) and predicates in adjacent lists
    are joined with an OR (_any()_). This allows for non-trivial filters to be
    written with just _tuples_ and _lists_.

    The predicates are in _tuples_ in the form (`key`, `op`, `value`) where the
    `key` is the value looked up from the record, the `op` is the operator and
    the `value` is a literal.
    """
    # No filter doesn't filter
    if predicate is None:
        return True

    # If we have a tuple extract out the key, operator and value
    # and do the evaluation
    if isinstance(predicate, tuple):
        key, op, value = predicate
        return OPERATORS[op](record.get(key), value)

    if isinstance(predicate, list):
        # Are all of the entries tuples?
        # We AND them together (_all_ are True)
        if all([isinstance(p, tuple) for p in predicate]):
            return all([evaluate(p, record) for p in predicate])

        # Are all of the entries lists?
        # We OR them together (_any_ are True)
        if all([isinstance(p, list) for p in predicate]):
            return any([evaluate(p, record) for p in predicate])

        # if we're here the structure of the filter is wrong
        raise InvalidSyntaxError('Unable to evaluate Filter')

    raise InvalidSyntaxError('Unable to evaluate Filter')


class Filters():

    __slots__ = ('empty_filter', 'filters')

    def __init__(
            self,
            filters: Optional[List[Tuple[str, str, object]]] = None):
        """
        A class to supporting filtering data.

        Parameters:
            filters: list of tuples
                Each tuple has format: (`key`, `op`, `value`). When run the
                filter will extract the `key` field from the dictionary and
                compare to the `value` using the operator `op`. Multiple
                filters are treated as AND, there is no OR at this time.
                The supported `op` values are: `=` or `==`, `!=`, `<`, `>`,
                `<=`, `>=`, `in`, `!in` (not in) and `like`. If the `op` is
                `in` or `!in`, the `value` must be a collection such as a
                _list_, a _set_ or a _tuple_.
                `like` performs similar to the SQL operator, `%` is a
                multi-character wildcard and `_` is a single character
                wildcard.

        Examples:
            filters = Filters([('name', '==', 'john')])
            filters = Filters([('name', '!=', 'john'),('name', '!=', 'tom')])

        """
        if filters:
            self.filters = filters
            self.empty_filter = False
        else:
            self.empty_filter = True


    def filter_dictset(
            self,
            dictset: Iterable[dict]) -> Iterable:
        """
        Tests each entry in a Iterable against the filters

        Parameters:
            dictset: iterable of dictionaries
                The dictset to process

        Yields:
            dictionary
        """
        for record in dictset:
            if self.empty_filter or evaluate(self.filters, record):
                yield record
