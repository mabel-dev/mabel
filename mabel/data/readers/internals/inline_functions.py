"""
These are a set of functions that can be applied to data as it passes through.

These are the function definitions, the processor which uses these is in the
'inline_evaluator' module.
"""
from ....utils.dates import parse_iso
import datetime
import fastnumbers


def get_year(input):
    """
    Convert input to a datetime object and extract the Year part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.year
    return None  #


def get_month(input):
    """
    Convert input to a datetime object and extract the Month part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.month
    return None


def get_day(input):
    """
    Convert input to a datetime object and extract the Day part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.day
    return None


def get_date(input):
    """
    Convert input to a datetime object and extract the Date part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.date()
    return None


def get_time(input):
    """
    Convert input to a datetime object and extract the Time part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.time()
    return None


def get_quarter(input):
    """
    Convert input to a datetime object and calculate the Quarter of the Year
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return ((input.month - 1) // 3) + 1
    return None


def get_hour(input):
    """
    Convert input to a datetime object and extract the Hour part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.hour
    return None


def get_minute(input):
    """
    Convert input to a datetime object and extract the Minute part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.minute
    return None


def get_second(input):
    """
    Convert input to a datetime object and extract the Seconds part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.second
    return None


def get_week(input):
    """
    Convert input to a datetime object and extract the ISO8601 Week
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.strftime("%V")
    return None



def concat(*items):
    """
    Turn each item to a string and concatenate the strings together
    """
    return "".join(map(str, items))


FUNCTIONS = {
    "YEAR": get_year,
    "MONTH": get_month,
    "DAY": get_day,
    "DATE": get_date,
    "QUARTER": get_quarter,
    "WEEK": get_week,
    "HOUR": get_hour,
    "MINUTE": get_minute,
    "SECOND": get_second,
    "TIME": get_time,
    "UCASE": lambda x: str(x).upper(),
    "UPPER": lambda x: str(x).upper(),
    "LCASE": lambda x: str(x).lower(),
    "LOWER": lambda x: str(x).lower(),
    "TRIM": lambda x: str(x).strip(),
    "LEN": len,
    "STRING": str,
    "LEFT": lambda x, y: str(x)[: int(y)],
    "RIGHT": lambda x, y: str(x)[-int(y) :],
    "MID": lambda x, y, z: str(x)[int(y) :][: int(z)],
    "CONCAT": concat,
    "ROUND": round,
    "TRUNC": fastnumbers.fast_int,
    "INT": fastnumbers.fast_int,
    "FLOAT": fastnumbers.fast_float,
    "BOOLEAN": lambda x: x.upper() != "FALSE",
    "ISNONE": lambda x: x is None,
}
