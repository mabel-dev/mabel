from ....utils.dates import parse_iso
import datetime


def get_year(input):
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.year
    return None  #


def get_month(input):
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.month
    return None


def get_day(input):
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.day
    return None


def get_date(input):
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.date()
    return None

def get_quarter(input):
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return ((input.month - 1) // 3) + 1
    return None


FUNCTIONS = {
    "YEAR": get_year,
    "MONTH": get_month,
    "DAY": get_day,
    "DATE": get_date,
    "QUARTER": get_quarter,
    "UCASE": lambda x: x.upper(),
    "LCASE": lambda x: x.lower(),
    "LEN": len,
    "ROUND": round,
    "TRUNC": int,
    "INT": int,
    "FLOAT": float,
    "BOOLEAN": lambda x: x.upper() != "FALSE",
    "ISNONE": lambda x: x is None,
}
