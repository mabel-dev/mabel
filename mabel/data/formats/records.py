from typing import List, Callable

def select_record_fields(
        record: dict,
        fields: List[str]) -> dict:
    """
    Selects a subset of fields from a dictionary. If the field is not present
    in the dictionary it defaults to None.

    Parameters:
        record: dictionary
            The dictionary to select from
        fields: list of strings
            The list of the field names to select

    Returns:
        dictionary
    """
    return {k: record.get(k, None) for k in fields}


def order(
        record: dict) -> dict:
    """
    Sort a dictionary by its keys.

    Parameters:
        record: dictionary
            The dictionary to sort

    Returns:
        dictionary
    """
    return dict(sorted(record.items()))


def set_value(
        record: dict,
        field_name: str,
        setter: Callable) -> dict:
    """
    Sets the value of a column to either a fixed value or as the result of a
    function which recieves the row as a parameter.

    Parameters:
        record: dictionary
            The dictionary to update
        field_name: string
            The field to create or update
        setter: callable or any
            A function or constant to update the field with

    Returns:
        dictionary
    """
    copy = record.copy()
    if callable(setter):
        copy[field_name] = setter(copy)
    else:
        copy[field_name] = setter
    return copy