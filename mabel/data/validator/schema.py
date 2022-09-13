import datetime
import decimal
import os

from typing import Any, Union, List, Dict

import orjson

from mabel.errors import ValidationError


def is_boolean(**kwargs):
    def _inner(value: Any) -> bool:
        """BOOLEAN"""
        if hasattr(value, "as_py"):
            value = value.as_py()
        return isinstance(value, bool) or value is None

    return _inner


def is_date(**kwargs):
    def _inner(value: Any) -> bool:
        """TIMESTAMP"""
        if hasattr(value, "as_py"):
            value = value.as_py()
        return isinstance(value, datetime.datetime) or value is None

    return _inner


def is_list(**kwargs):
    def _inner(value: Any) -> bool:
        """LIST"""
        if hasattr(value, "as_py"):
            value = value.as_py()
        if value is None:
            return True
        if isinstance(value, list):
            return all(type(i) == str for i in value)
        return False

    return _inner


def is_numeric(**kwargs):
    def _inner(value: Any) -> bool:
        """NUMERIC"""
        if hasattr(value, "as_py"):
            value = value.as_py()
        return isinstance(value, (int, float, decimal.Decimal)) or value is None

    return _inner


def is_string(**kwargs):
    def _inner(value: Any) -> bool:
        """VARCHAR"""
        if hasattr(value, "as_py"):
            value = value.as_py()
        return isinstance(value, str) or value is None

    return _inner


def is_struct(**kwargs):
    def _inner(value: Any) -> bool:
        """STRUCT"""
        if hasattr(value, "as_py"):
            value = value.as_py()
        return isinstance(value, dict) or value is None

    return _inner


def pass_anything(**kwargs):
    def _inner(value: Any) -> bool:
        """OTHER"""
        return True

    return _inner


"""
Create dictionaries to look up the type validators
"""
VALIDATORS = {
    "TIMESTAMP": is_date,
    "LIST": is_list,
    "VARCHAR": is_string,
    "BOOLEAN": is_boolean,
    "NUMERIC": is_numeric,
    "STRUCT": is_struct,
    "OTHER": pass_anything,
}


class Schema:
    def __init__(self, definition: Union[str, List[Dict[str, Any]], dict]):
        """
        Tests a dictionary against a schema to test for conformity.
        Schema definition is similar to - but not the same as - avro schemas

        Paramaters:
            definition: dictionary or string
                A dictionary, a JSON string of a dictionary or the name of a
                JSON file containing a schema definition
        """
        # typing system struggles to understand what is happening here

        # if we have a schema as a string, load it into a dictionary
        if isinstance(definition, str):
            if os.path.exists(definition):  # type:ignore
                definition = orjson.loads(
                    open(definition, mode="r").read()
                )  # type:ignore
            else:
                definition = orjson.loads(definition)  # type:ignore

        if isinstance(definition, dict):
            if definition.get("fields"):  # type:ignore
                definition = definition["fields"]  # type:ignore

        self.definition = definition

        try:
            # read the schema and look up the validators
            self._validators = {  # type:ignore
                item.get("name"): VALIDATORS[item.get("type")]()  # type:ignore
                for item in definition  # type:ignore
            }

        except KeyError as e:
            print(e)
            raise ValueError(
                f"Invalid type specified in schema - {e}. Valid types are: {', '.join(VALIDATORS.keys())}"
            )
        if len(self._validators) == 0:
            raise ValueError("Invalid schema specification")

        self._validator_columns = set(self._validators.keys())

    def _field_validator(self, value, validator) -> bool:
        """
        Execute a set of validator functions (the _is_x) against a value.
        Return True if any of the validators are True.
        """
        if validator is None:
            return True
        return validator(value)

    def validate(self, subject: dict, raise_exception=False) -> bool:
        """
        Test a dictionary against the Schema

        Parameters:
            subject: dictionary
                The dictionary to test for conformity
            raise_exception: boolean (optional, default False)
                If True, when the subject doesn't conform to the schema a
                ValidationError is raised

        Returns:
            boolean, True is subject conforms

        Raises:
            ValidationError
        """
        result = True
        self.last_error = ""

        # find columns in the data, not in the schema
        # Note: fields in the schema but not in the data passes schema validation
        additional_columns = set(subject.keys()) - self._validator_columns
        if len(additional_columns) > 0:
            self.last_error += f"Column names in record not found in Schema - {', '.join(additional_columns)}"
            result = False

        for key, value in self._validators.items():
            if not self._field_validator(subject.get(key), value):
                result = False
                self.last_error += f"'{key}' (`{subject.get(key)}`) did not pass `{value.__doc__}` validator.\n"
        if raise_exception and not result:
            raise ValidationError(
                f"Record does not conform to schema - {self.last_error}. "
            )
        return result

    @property
    def columns(self):
        return self._validator_columns

    def __call__(self, subject: dict = {}, raise_exception=False) -> bool:
        """
        Alias for validate
        """
        return self.validate(subject=subject, raise_exception=raise_exception)

    def __str__(self):
        retval = []
        for key, value in self._validators.items():
            val = [str(v).split(".")[0].split(" ")[1] for v in value]
            val = ",".join(val)
            retval.append({"field": key, "type": val})

        return orjson.dumps(retval).decode()

    def __contains__(self, name):
        """we can short cut this check"""
        return name in self._validators

    def __getitem__(self, name):
        """to avoid errors, do `name in schema` first"""
        for item in self.definition:
            if item.get("name") == name:
                return item.get("type")
        raise IndexError(f"'{name}' not in schema")

    def get(self, name, default=None):
        if name in self:
            return self[name]
        return default
