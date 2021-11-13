import functools
import inspect
from ..logging import get_logger
from ..errors import InvalidReaderConfigError


def get_levenshtein_distance(word1, word2):
    """
    https://en.wikipedia.org/wiki/Levenshtein_distance
    :param word1:
    :param word2:
    :return:
    """
    word2 = word2.lower()
    word1 = word1.lower()
    matrix = [[0 for x in range(len(word2) + 1)] for x in range(len(word1) + 1)]

    for x in range(len(word1) + 1):
        matrix[x][0] = x
    for y in range(len(word2) + 1):
        matrix[0][y] = y

    for x in range(1, len(word1) + 1):
        for y in range(1, len(word2) + 1):
            if word1[x - 1] == word2[y - 1]:
                matrix[x][y] = min(
                    matrix[x - 1][y] + 1, matrix[x - 1][y - 1], matrix[x][y - 1] + 1
                )
            else:
                matrix[x][y] = min(
                    matrix[x - 1][y] + 1, matrix[x - 1][y - 1] + 1, matrix[x][y - 1] + 1
                )

    return matrix[len(word1)][len(word2)]


class validate:  # pragma: no cover
    def __init__(self, rules):
        self.rules = rules

    def add_rule(self, rule):
        self.rules.append(rule)

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            has_errors = False

            my_rules = self.rules.copy()

            # get the names for all of the parameters
            func_parameters = list(inspect.signature(func).parameters)
            entered_parameters = kwargs.copy()
            for index, parameter in enumerate(args):
                entered_parameters[func_parameters[index]] = parameter

            # add rules from parameters
            for new_rules in [
                v.RULES for k, v in entered_parameters.items() if hasattr(v, "RULES")
            ]:
                my_rules += new_rules

            # check for spelling mistakes
            valid_parameters = {item["name"] for item in my_rules}
            for not_on_list in [
                item for item in entered_parameters if item not in valid_parameters
            ]:
                suggestion = []
                for valid in valid_parameters:
                    if get_levenshtein_distance(not_on_list, valid) <= (
                        len(valid) // 2
                    ):
                        suggestion.append(valid)
                if len(suggestion):
                    get_logger().error(
                        {
                            "error": "unknown parameter",
                            "function": func.__qualname__,
                            "parameter": not_on_list,
                            "closest valid option(s)": suggestion,
                        }
                    )
                else:
                    get_logger().error(
                        {
                            "error": "unknown parameter",
                            "function": func.__qualname__,
                            "parameter": not_on_list,
                            "supported_options": f"{valid_parameters}",
                        }
                    )
                has_errors = True

            # check for missing required paramters
            required_parameters = {
                item.get("name") for item in my_rules if item.get("required")
            }
            missing_required_parameters = [
                param
                for param in required_parameters
                if param not in entered_parameters
            ]
            if len(missing_required_parameters):
                get_logger().error(
                    {
                        "error": "missing required parameters",
                        "function": func.__qualname__,
                        "missing_paramters": missing_required_parameters,
                    }
                )
                has_errors = True

            # warnings
            warninged_parameters = {
                item.get("name"): item.get("warning")
                for item in my_rules
                if item.get("warning")
            }
            for parameter, warning in warninged_parameters.items():
                if parameter in entered_parameters:
                    get_logger().warning(warning)

            # toxic
            for param in [rule for rule in self.rules if rule.get("incompatible_with")]:
                if (
                    param.get("name") in entered_parameters
                    and entered_parameters[param.get("name")]
                ):
                    toxic = [
                        t
                        for t in param["incompatible_with"]
                        if t in entered_parameters.keys()
                    ]
                    if toxic:
                        has_errors = True
                        get_logger().error(
                            {
                                "error": "invalid combination of parameters",
                                "function": func.__qualname__,
                                "parameter": param.get("name", ""),
                                "incompatible with": toxic,
                            }
                        )

            if has_errors:
                raise InvalidReaderConfigError("Reader has invalid parameters")

            return func(*args, **kwargs)

        return wrapper
