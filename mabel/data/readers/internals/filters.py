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
from typing import Optional, Any, Iterable, List, Tuple
import re


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

class Filters():

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
        else:
            # if there's no filter, always pass
            setattr(self, 'test_record', true)

    def test_record(
            self,
            record: dict) -> bool:
        """
        Tests a value against the filters

        Parameters:
            record: dictionary
                The record to test

        Returns:
            boolean
        """
        try:
            for key, op, value in self.filters:
                if not (OPERATORS[op](record.get(key), value)):
                    return False
            return True
        except:
            return False

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
            if self.test_record(record):
                yield record



def _inner(filters, data):  # pragma: no cover

    if isinstance(filters, tuple):
        key, op, value = filters
        return OPERATORS[op](record.get(key), value)

    if isinstance(filters, list):
        for _filter in filters:
            # if I'm made of tuples, AND them
            if isinstance(_filter, tuple):
                r.append(_inner(_filter, data))
            # if I'm made of lists, OR them
            elif isinstance(_filter, list):
                r.append(_inner(_filter, data))
            
    return False

if __name__ == "__main__":  # pragma: no cover

    