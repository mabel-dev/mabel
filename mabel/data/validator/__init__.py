import os
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import orjson
from orso.schema import RelationSchema

__all__ = ["schema_loader"]


def schema_loader(
    definition: Union[str, List[Dict[str, Any]], dict, RelationSchema, bool]
) -> Union[RelationSchema, bool]:
    if definition is None:
        raise ValueError(
            "Writer is missing a schema, minimum schema is a list of the columns or explicitly set to 'False' is data is unschemable."
        )

    if definition is False:
        return False

    if isinstance(definition, RelationSchema):
        return definition

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
    definition["name"] = definition.get("name", "wal")  # type:ignore

    return RelationSchema.from_dict(definition)
