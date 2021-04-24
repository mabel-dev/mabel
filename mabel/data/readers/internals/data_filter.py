from functools import lru_cache
import re


# https://codereview.stackexchange.com/a/248421
_special_regex_chars = {
    ch : '\\'+ch
    for ch in '.^$*+?{}[]|()\\'
}

@lru_cache(4)
def _sql_like_fragment_to_regex(fragment):
    # https://codereview.stackexchange.com/a/36864/229677
    safe_fragment = ''.join([_special_regex_chars.get(ch, ch) for ch in fragment])
    return re.compile('^' + safe_fragment.replace('%', '.*?').replace('_', '.') + '$')

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

# functions which implement the Operators
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

class DataFilter():

    def __init__(self, filters: None):
        if filters:
            self.filters = filters
        else:
            # if there's no filter, always pass
            setattr(self, 'test', lambda x: True)

    def test(self, row):
        for key, op, value in self.filters:
            if not (OPERATORS.get(op)(row.get(key), value)):
                return False
        return True

    def filter_list(self, lst):
        for row in lst:
            if self.test(row):
                yield row


if __name__ == "__main__":


    TEST_DATA = [
        { "name": "Sirius Black", "age": 40, "dob": "1970-01-02", "gender": "male" },
        { "name": "Harry Potter", "age": 11, "dob": "1999-07-30", "gender": "male" },
        { "name": "Hermione Grainger", "age": 10, "dob": "1999-12-14", "gender": "female" },
        { "name": "Fleur Isabelle Delacour", "age": 11, "dob": "1999-02-08", "gender": "female" },
        { "name": "James Potter", "age": 40, "dob": "1971-12-30", "gender": "male" },
        { "name": "James Potter", "age": 0, "dob": "2010-12-30", "gender": "male" }
    ]

    d = DataFilter(filters=[('age', '==', 11), ('gender', 'in', ('a', 'b', 'male'))])

    for entry in TEST_DATA:
        if d.test(entry):
            print(entry)