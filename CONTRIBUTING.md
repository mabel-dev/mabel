# Contribution Guide


Code should look like:
- Imports on separate lines (imports then froms)
- Four space tabs
- Variables should be in `snake_case`
- Classes should be in `PascalCase`
- Constants should be in `UPPER_CASE`
- Methods with docstrings
- Style informed by PEP8 (with a relaxed view on line lengths)
- defs or calls with many parameters have them on different lines
- Type hints
- Self-explanatory method, class and variable names


Code should have:
- Corresponding unit/regression tests
- Attributed external sources - even if there is no explicit license requirement, The context of the source may help others reading the code later.


A note about comments:
- Computers will interpret anything, humans need help interpreting code
- Prefer readable code over verbose comments
- Humans struggle with threading, recursion, parallelization, variables called 'x' and more than 10, of anything
- Comments should be more than just the code in other words
- Good variable names and well-written code doesn't need comments


Pull requests should pass:
- bandit (secure coding practices)
- mypy (type hints)
- pytest (regression tests)
- test coverage should not be reduced (current bar is 80%)
- maintainability index for each module above 50


Check-ins should have prefixes:
- `[MBL-nnn]` items relating to Jira tickets
- `[FIX]` items fixing bugs
- `[TEST]` improvements to testing


Docstrings should look like:
~~~python
def sample_method(param_1, param_2):
"""
A short description of what the method does.

Parameters:
    param_1: string
        describe this parameter
    param_2: integer (optional)
        describe this parameter, if it's optional, what is the default

Returns:
    boolean
"""
~~~
