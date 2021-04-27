"""
Validator Tests
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data.validator import Schema
from mabel.data.formats.json import serialize
from mabel.errors import ValidationError
from rich import traceback

traceback.install()


def test_validator_all_valid_values():

    TEST_DATA = { 
        "string_field": "string",
        "integer_field": 100,
        "boolean_field": True,
        "date_field": datetime.datetime.today(),
        "other_field": ["abc"],
        "nullable_field": None,
        "list_field": ['a', 'b', 'c'],
        "enum_field": "RED"
    }
    TEST_SCHEMA = {
        "fields": [
            {"name": "string_field",   "type": "string"},
            {"name": "str_null_field", "type": ["string", "nullable"]},
            {"name": "integer_field",  "type": "numeric"},
            {"name": "boolean_field",  "type": "boolean"},
            {"name": "date_field",     "type": "date"},
            {"name": "other_field",    "type": "other"},
            {"name": "nullable_field", "type": "nullable"},
            {"name": "list_field",     "type": "list"},
            {"name": "enum_field",     "type": "enum",   "symbols": ['RED', 'GREEN', 'BLUE']}
        ]
    }

    test = Schema(TEST_SCHEMA)
    assert (test.validate(TEST_DATA))


def test_validator_invalid_string():

    TEST_DATA = { "string_field": 100 }
    TEST_SCHEMA = { "fields": [ { "name": "string_field", "type": "string" } ] }

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(TEST_DATA))


def test_validator_invalid_number():

    TEST_DATA = { "number_field": "one hundred" }
    TEST_SCHEMA = { "fields": [ { "name": "number_field", "type": "numeric" } ] }

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(TEST_DATA))

    TEST_DATA = { "number_field": print }
    TEST_SCHEMA = { "fields": [ { "name": "number_field", "type": "numeric" } ] }

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(TEST_DATA))


def test_validator_invalid_schema():

    result = True
    try:
        Schema({"name": "string"})
    except:  # pragma: no cover
        result = False
    assert (not result)
    

def test_validator_invalid_boolean():

    TEST_DATA = { "boolean_field": "not true" }
    TEST_SCHEMA = { "fields": [ { "name": "boolean_field", "type": "boolean" } ] }

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(TEST_DATA))


def test_validator_multiple_types():

    TEST_DATA_1 = { "multi": "True" }
    TEST_DATA_2 = { "multi": True }
    TEST_DATA_3 = { "multi": None }
    TEST_SCHEMA = { "fields": [ { "name": "multi", "type": ["string", "boolean", "nullable"] } ] }

    test = Schema(TEST_SCHEMA)
    assert (test.validate(TEST_DATA_1))
    assert (test.validate(TEST_DATA_2))
    assert (test.validate(TEST_DATA_3))


def test_validator_nonnative_types():

    TEST_DATA = { 
        "integer_field": "100",
        "boolean_field": "True",
        "date_field": "2000-01-01T00:00:00.000",
        "nullable_field": ""
    }
    TEST_SCHEMA = {
        "fields": [
            { "name": "integer_field",  "type": "numeric" },
            { "name": "boolean_field",  "type": "boolean" },
            { "name": "date_field",     "type": "date"    },
            { "name": "nullable_field", "type": "nullable"}
        ]
    }

    test = Schema(TEST_SCHEMA)
    assert (test.validate(TEST_DATA)), test.last_error


def test_validator_extended_schema():
    """
    Ensure the validator will ignore additional fields in the schema
    """
    TEST_DATA = { "string_field": "the" }
    TEST_SCHEMA = {
        "table": "this is a test schema",
        "fields": [ 
            { 
                "name": "string_field", 
                "type": "string", 
                "description": "character array", 
                "last_updated": datetime.datetime.today() 
            } 
        ] 
    }

    test = Schema(TEST_SCHEMA)
    assert (test.validate(TEST_DATA))


def test_validator_loaders():
    """
    Ensure dictionary, json and json files load
    """

    TEST_SCHEMA_DICT = {"fields": [{"name": "string_field", "type": "string"}]}
    TEST_SCHEMA_STRING = serialize(TEST_SCHEMA_DICT)
    TEST_SCHEMA_FILE = 'temp'

    with open(TEST_SCHEMA_FILE, 'w') as file:
        file.write(TEST_SCHEMA_STRING)

    failed = False
    try:
        test = Schema(TEST_SCHEMA_DICT)
        test.validate({"string_field": "pass"})
    except Exception:  # pragma: no cover
        failed = True
    assert not failed, "load schema from dictionary"

    failed = False
    try:
        test = Schema(TEST_SCHEMA_STRING)
        test.validate({"string_field": "pass"})
    except Exception:  # pragma: no cover
        failed = True
    assert not failed, "load schema from string"

    failed = False
    try:
        test = Schema(TEST_SCHEMA_FILE)
        test.validate({"string_field": "pass"})
    except Exception:  # pragma: no cover
        failed = True
    assert not failed, "load schema from file"


def test_validator_list():

    INVALID_TEST_DATA = {"key": "not a list"}
    VALID_TEST_DATA = {"key": ["is", "a", "list"]}
    TEST_SCHEMA = {"fields": [{"name": "key", "type": "list"}]}

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(INVALID_TEST_DATA))
    assert (test.validate(VALID_TEST_DATA))


def test_validator_enum():

    INVALID_TEST_DATA = {"key": "left"}
    VALID_TEST_DATA = {"key": "north"}
    TEST_SCHEMA = {"fields": [{"name": "key", "type": "enum", "symbols": ['north', 'south']}]}

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(INVALID_TEST_DATA))
    assert (test.validate(VALID_TEST_DATA))


def test_validator_date():

    INVALID_TEST_DATA_1 = {"key": "tomorrow"}
    INVALID_TEST_DATA_2 = {"key": "2020001001"}
    INVALID_TEST_DATA_3 = {"key": "2020-00-01"}
    VALID_TEST_DATA = {"key": "2020-01-01"}
    TEST_SCHEMA = {"fields": [{"name": "key", "type": "date"}]}

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(INVALID_TEST_DATA_1))
    assert (not test.validate(INVALID_TEST_DATA_2))
    assert (not test.validate(INVALID_TEST_DATA_3))
    assert (test.validate(VALID_TEST_DATA))


def test_unknown_type():

    TEST_SCHEMA = {"fields": [{"name": "key", "type": "not_a_known_type"}]}

    failed = False
    try:
        test = Schema(TEST_SCHEMA)
    except ValueError:  # pragma: no cover
        failed = True

    assert failed


def test_raise_exception():

    TEST_DATA = { "number_field": "one hundred" }
    TEST_SCHEMA = { "fields": [ { "name": "number_field", "type": "numeric" } ] }

    test = Schema(TEST_SCHEMA)
    failed = False
    try:
        test.validate(TEST_DATA, raise_exception=True)
    except ValidationError:  # pragma: no cover
        failed = True

    assert failed


def test_call_alias():

    TEST_DATA = { "number_field": 100 }
    TEST_SCHEMA = { "fields": [ { "name": "number_field", "type": "numeric" } ] }

    test = Schema(TEST_SCHEMA)
    assert test(TEST_DATA)


def test_validator_number_ranges():

    OVER_TEST_DATA = {"number": 1000}
    UNDER_TEST_DATA = {"number": 100}
    IN_TEST_DATA = {"number": 500}
    TEST_SCHEMA = {"fields": [{"name": "number", "type": "numeric", "min": 250, "max": 750}]}

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(OVER_TEST_DATA))
    assert (not test.validate(UNDER_TEST_DATA))
    assert (test.validate(IN_TEST_DATA))

    TEST_SCHEMA_MIN = {"fields": [{"name": "number", "type": "numeric", "min": 250}]}
    test = Schema(TEST_SCHEMA_MIN)
    assert (test.validate(OVER_TEST_DATA)), test.last_error
    assert not (test.validate(UNDER_TEST_DATA)), test.last_error

    TEST_SCHEMA_MAX = {"fields": [{"name": "number", "type": "numeric", "max": 750}]}
    test = Schema(TEST_SCHEMA_MAX)
    assert (test.validate(UNDER_TEST_DATA)), test.last_error
    assert not (test.validate(OVER_TEST_DATA)), test.last_error


def test_validator_string_format():

    INVALID_TEST_DATA = {"cve": "eternalblue"}
    VALID_TEST_DATA = {"cve": "CVE-2017-0144"}
    TEST_SCHEMA = {"fields": [{"name": "cve", "type": "string", "format": r"(?i)CVE-\d{4}-\d{4,7}"}]}

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(INVALID_TEST_DATA))
    assert (test.validate(VALID_TEST_DATA)), test.last_error


def test_validator_cve_format():

    INVALID_TEST_DATA = {"cve": "eternalblue"}
    VALID_TEST_DATA = {"cve": "CVE-2017-0144"}
    TEST_SCHEMA = {"fields": [{"name": "cve", "type": "cve"}]}

    test = Schema(TEST_SCHEMA)
    assert (not test.validate(INVALID_TEST_DATA))
    assert (test.validate(VALID_TEST_DATA)), test.last_error


if __name__ == "__main__":  # pragma: no cover
    test_validator_all_valid_values()
    test_validator_invalid_string()
    test_validator_invalid_number()
    test_validator_invalid_schema()
    test_validator_invalid_boolean()
    test_validator_multiple_types()
    test_validator_nonnative_types()
    test_validator_extended_schema()
    test_validator_loaders()
    test_validator_list()
    test_validator_enum()
    test_validator_number_ranges()
    test_validator_string_format()
    test_validator_date()
    test_unknown_type()
    test_raise_exception()
    test_call_alias()
    test_validator_cve_format()

    print('okay')
