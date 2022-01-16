"""
End Operator

Explicit marker for the end of a flow.

The EndOperator has no configuration options, it is currently implemented to
terminate flows - although this is mostly a passive activity, done by
returning 'None'.

The purpose of this Operator is three-fold:

- Makes it clear to human readers where flows end
- Prevents issues with the flow builder
- Provides opportunity in the future to introduce functionality at the 
  completion of a flow
"""
from ..flows.internals.base_operator import BaseOperator


class EndOperator(BaseOperator):
    def execute(self, data={}, context={}):
        # do nothing
        pass
