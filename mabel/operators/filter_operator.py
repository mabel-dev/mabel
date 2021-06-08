from ..flows.internals.base_operator import BaseOperator


def match_all(data):
    return True


class FilterOperator(BaseOperator):
    def __init__(self, condition=None):
        """
        Filters records, returns the record for matching records and returns 'None'
        for non-matching records.

        The Filter Operator takes one configuration item at creation - a _Callable_
        (function) which takes the data as a dictionary as it's only parameter and
        returns `True` to retain the record, or `False` to not pass the record
        through to the next Operator.

        Example:
            fo = FilterOperator(condition=lambda r: r.get('severity') == 'high')

        Note:
            The condition does not need to be lambda, it can be any _Callable_
            including methods.
        """
        super().__init__()
        if not condition:
            self.logger.warning(
                "FilterOperator missing expected named parameter `condition`."
            )
            condition = match_all
        self.condition = condition

    def execute(self, data={}, context={}):
        if self.condition(data):
            return data, context
        return None
