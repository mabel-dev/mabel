import os
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import orjson
from orso.schema import RelationSchema


def schema_loader(
    definition: Union[str, List[Dict[str, Any]], dict, RelationSchema]
) -> RelationSchema:
    if definition is None:
        raise ValueError("Writer is missing a schema, minimum schema is a list of the columns.")

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
