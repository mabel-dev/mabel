import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.operators import EndOperator
from mabel.errors import IntegrityError
from mabel import BaseOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

fail_counter = 0

class failing_operator(BaseOperator):
    """ create an Operator which always fails """
    def execute(self, data, context): 
        global fail_counter
        fail_counter += 1
        raise Exception('Failure')
        return data, context
        
def test_retry():
    flow = failing_operator(retry_count=3, retry_wait=1) > EndOperator()
    with flow as runner:
        runner.run(data='', context={})
    global fail_counter
    assert fail_counter == 3, fail_counter

if __name__ == "__main__":
    test_retry()

    print('okay')
