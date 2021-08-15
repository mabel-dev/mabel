import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import pytest
from mabel.data.readers.internals.sql_reader import SqlParser
from rich import traceback

traceback.install()


def test_parser():

    STATEMENTS = [
        {"SQL": "SELECT * FROM TABLE", "select": ["*"], "from": "TABLE"},
        {"SQL": "SELECT value FROM TABLE", "select": ["value"], "from": "TABLE"},
        {
            "SQL": "SELECT value1, value2 FROM TABLE",
            "select": ["value1", "value2"],
            "from": "TABLE",
        },
        {
            "SQL": "SELECT\n\tvalue\nFROM\nTABLE\nWHERE\n\tvalue == 1",
            "select": ["value"],
            "from": "TABLE",
            "where": "value == 1",
        },
        {
            "SQL": "SELECT COUNT(*) FROM TABLE WHERE value == 1 GROUP BY value LIMIT 3",
            "select": [("COUNT", "*")],
            "from": "TABLE",
            "where": "value == 1",
            "group_by": ["value"],
            "limit": 3,
        },
        {
            "SQL": "SELECT \n    MAX(cve.CVE_data_meta.ID),\n    MIN(cve.CVE_data_meta.ID),\n    COUNT(cve.CVE_data_meta.ID) \nFROM mabel_data.RAW.NVD.CVE_LIST GROUP BY cve.CVE_data_meta.ASSIGNER",
            "select": [
                ("MAX", "cve.CVE_data_meta.ID"),
                ("MIN", "cve.CVE_data_meta.ID"),
                ("COUNT", "cve.CVE_data_meta.ID"),
            ],
            "from": "mabel_data/RAW/NVD/CVE_LIST",
            "group_by": ["cve.CVE_data_meta.ASSIGNER"],
        },
    ]

    for statement in STATEMENTS:
        # print(statement["SQL"])
        parsed = SqlParser(statement["SQL"])
        assert parsed.select == statement.get(
            "select"
        ), f"SELECT {statement.get('select')}: {parsed}"
        assert parsed._from == statement.get("from"), f"FROM: {parsed}"
        assert parsed.where == statement.get("where"), f"WHERE: {parsed}"
        assert parsed.group_by == statement.get("group_by"), f"GROUP BY: {parsed}"
        assert parsed.having == statement.get("having"), f"HAVING: {parsed}"
        assert parsed.limit == statement.get("limit"), f"LIMIT: {parsed}"


def test_invalid_sql():
    STATEMENTS = [
        "SELECT * FROM TABLE WHERE value == 1 GROUP BY value LIMIT 3",
        "SELECT *",
    ]

    for statement in STATEMENTS:
        # print(statement)
        with pytest.raises(Exception):
            parsed = SqlParser(statement)


if __name__ == "__main__":

    test_parser()
    test_invalid_sql()

    print("complete")
