import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.operators import EndOperator
from mabel import BaseOperator, operator
from rich import traceback

traceback.install()

DATASET = [{"secret": "123"}, {"secret": "456"}]


@operator
def do_something(data):
    data["secret"] = hash(data.get("secret"))
    print(data)
    return data


def test_operatify():

    error = False

    try:
        f = do_something > EndOperator()
        with f as runner:
            for row in DATASET:
                runner(row)
    except:
        error = True

    assert error == False


if __name__ == "__main__":  # pragma: no cover
    test_operatify()

    print("okay")
