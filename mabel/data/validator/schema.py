import os
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import orjson
from mabel.errors import ValidationError
from orso.schema import RelationSchema


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
                definition = orjson.loads(open(definition, mode="r").read())  # type:ignore
            else:
                definition = orjson.loads(definition)  # type:ignore

        if isinstance(definition, dict):
            if definition.get("fields"):  # type:ignore
                definition["columns"] = definition.pop("fields")  # type:ignore

        if isinstance(definition, list):
            definition = {"columns": definition}
        definition["name"] = definition.get("name", "wal")

        self.schema = RelationSchema.from_dict(definition)

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

        try:
            self.schema.validate(subject)
        except Exception as err:
            self.last_error = str(err)
            if raise_exception:
                raise ValidationError(err) from err
            return False

        return True

    @property
    def columns(self):
        return self.schema.columns

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
