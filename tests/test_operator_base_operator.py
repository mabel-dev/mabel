import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.operators import EndOperator
from mabel.errors import IntegrityError
from mabel import BaseOperator
from rich import traceback

traceback.install()

fail_counter = 0


class failing_operator(BaseOperator):
    """create an Operator which always fails"""

    def execute(self, data, context):
        global fail_counter
        fail_counter += 1
        raise Exception("Failure")
        return data, context


def test_retry():
    """test the retry runs the specified amount of time"""
    flow = failing_operator(retry_count=3, retry_wait=1) > EndOperator()
    with flow as runner:
        runner(data="", context={})
    global fail_counter
    assert fail_counter == 3, fail_counter


def test_sigterm():
    """test sigterm is constant"""
    sig_terms = []
    for i in range(3):
        time.sleep(1)
        sig_terms.append(BaseOperator.sigterm())
    for i in range(3):
        time.sleep(1)
        sig_terms.append(failing_operator.sigterm())
    assert all(
        [sig_terms[i] == sig_terms[i + 1] for i in range(len(sig_terms) - 1)]
    ), sig_terms


if __name__ == "__main__":  # pragma: no cover
    test_retry()
    test_sigterm()

    print("okay")
