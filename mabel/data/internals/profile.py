
"""
// .profile data (Zone Maps)
- per dataset:
    - columns [type, min, max, count, number unique, sum, number empty]
        - list of columns allows is to use lists of tuples instead of dicts
        - count/min/max/average allows us to quick return simple whole dataset aggregations
- per partition:
    - columns [min, max, count]
        - min/max allows us to skip files if the value isn't in the file
"""


"""
We create a ZoneMap by profiling the dataset.
"""

{
    "dataset": "name",
    "columns": {
        "[column_name]": {
            "type": "int",
            "min": 0,
            "max": 0,
            "count": 0,
            "unique": 0,
            "sum": 0,
            "empty": 0
        }
    },
    "partitions": {
        "[filename]": {
            "columns": {
                "[columns_name]": {
                    "min": 0,
                    "max": 0,
                    "count": 0,
                }
            }
        }
    }
}