"""
Query Planner
-------------

This builds a DAG which describes a query.

This doesn't attempt to do optimization, this just decomposes the query.
"""

NODE_TYPES = {
    "select": "select",     # filter tuples
    "project": "project", # filter fields
    "read": "read",  # read file
    "join": "join",  # cartesian join
    "union": "union",  # acculate records 
    "rename": "rename",  # rename fields
    "sort": "sort",  # order records
    "#aggregation": "aggregation",  # calculate aggregations
    "#evaluation": "evaluation"  # calculate evaluations
}

## expressions are split, ANDs are separate items, ORs are kept together

"""

SELECT * FROM TABLE => reader(TABLE) -> project(*)
SELECT field FROM TABLE => reader(TABLE) -> project(field)
SELECT field FROM TABLE WHERE field > 7 => reader(TABLE) -> select(field > 7) -> project(field)

"""