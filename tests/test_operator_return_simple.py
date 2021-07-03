import pytest
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.logging import get_logger
from mabel.operators import FilterOperator, EndOperator, NoOpOperator
from mabel.flows import Flow
from rich import traceback

traceback.install()


class SimpleReturnOperator(NoOpOperator):
    def execute(self, data={}, context={}):
        """
        Return a simple (single) value to ensure a ValueError is raised
        """
        return True


def test_fails_on_simple_return():

    simp = SimpleReturnOperator()
    end = EndOperator()

    flow = simp >> end

    with pytest.raises(ValueError):
        with flow as runner:
            runner({}, {})


if __name__ == "__main__":  # pragma: no cover

    test_fails_on_simple_return()
    print("okay")
