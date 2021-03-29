"""
Test that the schema summarizer is mapping the optional fields
and the simplified types correctly
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.data.validator import Schema
from mabel.errors import ValidationError
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def stest_validator_summary_schema():

    TEST_SCHEMA = {
        "fields": [
            {"name": "string_field",   "type": "string",   "description": "abc"},
            {"name": "str_null_field", "type": ["string", "nullable"]},
            {"name": "null_str_field", "type": ["nullable", "string"]},
            {"name": "cve_field",      "type": "cve"},
            {"name": "integer_field",  "type": "numeric"},
            {"name": "int_m_m_field",  "type": "numeric", "min": 0, "max": 100},
            {"name": "boolean_field",  "type": "boolean"},
            {"name": "date_field",     "type": "date"},
            {"name": "other_field",    "type": "other"},
            {"name": "nullable_field", "type": "nullable"},
            {"name": "list_field",     "type": "list"},
            {"name": "enum_field",     "type": "enum",   "symbols": ['RED', 'GREEN', 'BLUE']}
        ]
    }

    schema = Schema(TEST_SCHEMA).field_types()

    assert schema['string_field'] == ['string']
    assert schema['str_null_field'] == ['string']
    assert schema['null_str_field'] == ['string']
    assert schema['cve_field'] == ['string']
    assert schema['integer_field'] == ['numeric']
    assert schema['int_m_m_field'] == ['numeric']
    assert schema['boolean_field'] == ['enum']
    assert schema['date_field'] == ['date']
    assert schema['other_field'] == []
    assert schema['nullable_field'] == []
    assert schema['list_field'] == ['list']
    assert schema['enum_field'] == ['enum']


if __name__ == "__main__":
    stest_validator_summary_schema()
    print('okay')
