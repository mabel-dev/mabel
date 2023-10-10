import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import DatabaseWriter
from mabel.data import Reader
from rich import traceback
from mabel.adapters.database import NullWriter

traceback.install()


def test_database_writer_simple():
    w = DatabaseWriter(
        dataset="_temp",
        schema=[{"name": "character", "type": "VARCHAR"}, {"name": "persona", "type": "VARCHAR"}],
        inner_writer=NullWriter,
        writer_config={},
    )
    for i in range(int(1e5)):
        w.append({"character": "Barney Stinson", "persona": "Lorenzo Von Matterhorn"})
        w.append({"character": "Laszlo Cravensworth", "persona": "Jackie Daytona"})
    number_of_rows = w.finalize()

    assert (
        number_of_rows == 2e5
    ), f"unique database writer expected {2e5} records, found {number_of_rows}"


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    test_database_writer_simple()
    run_tests()
