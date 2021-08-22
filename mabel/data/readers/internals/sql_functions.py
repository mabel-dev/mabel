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
        return input.date
    return None


FUNCTIONS = {
    "YEAR": get_year,
    "MONTH": get_month,
    "DAY": get_day,
    "DATE": get_date,
    "UCASE": lambda x: x.upper(),
    "LCASE": lambda x: x.lower(),
    "LEN": len,
    "ROUND": round,
    "TRUNC": int,
    "INT": int,
    "FLOAT": float
}
