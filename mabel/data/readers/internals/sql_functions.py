from ....utils.dates import parse_iso
import datetime
import fastnumbers


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


def get_week(input):
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.strftime("%V")
    return None


FUNCTIONS = {
    "YEAR": get_year,
    "MONTH": get_month,
    "DAY": get_day,
    "DATE": get_date,
    "QUARTER": get_quarter,
    "WEEK": get_week,
    "UCASE": lambda x: str(x).upper(),
    "LCASE": lambda x: str(x).lower(),
    "TRIM": lambda x: str(x).strip(),
    "LEN": len,
    "ROUND": round,
    "TRUNC": fastnumbers.fast_int,
    "INT": fastnumbers.fast_int,
    "FLOAT": fastnumbers.fast_float,
    "BOOLEAN": lambda x: x.upper() != "FALSE",
    "ISNONE": lambda x: x is None,
    "LEFT": lambda x, y: x[:y]
}
