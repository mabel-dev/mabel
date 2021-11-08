import re
import datetime
from fastnumbers import fast_int
from typing import Optional, Union

TIMEDELTA_REGEX = (
    r"((?P<months>-?\d+)mo)?"
    r"((?P<days>-?\d+)d)?"
    r"((?P<hours>-?\d+)h)?"
    r"((?P<minutes>-?\d+)m)?"
    r"((?P<seconds>-?\d+)s)?"
)
TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)


def extract_date(value):
    if isinstance(value, str):
        value = parse_iso(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return datetime.date(value.year, value.month, value.day)
    return datetime.date.today()

def add_months(date, months):
    new_month = (((date.month - 1) + months) % 12) + 1
    new_year = int(date.year + (((date.month - 1) + months) / 12))
    new_day = date.day

    # if adding one day puts us in a new month, jump to the end of the month
    if (date + datetime.timedelta(days=1)).month != date.month:
        new_day = 31

    # not all months have 31 days so walk backwards to the end of the month
    while new_day > 0:
        try:
            new_date = datetime.date(new_year, new_month, new_day)
            return date - new_date
        except ValueError:
            new_day -= 1

    # we should never be here - but just return a value
    return date - datetime.date(new_year, new_month, date.day)

# based on:
# https://gist.github.com/santiagobasulto/698f0ff660968200f873a2f9d1c4113c#file-parse_timedeltas-py
def parse_delta(delta: str) -> datetime.timedelta:
    """
    Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.

    Delta includes:
    * Xmo months
    * Xd days
    * Xh hours
    * Xm minutes
    * Xs seconds

    Values can be negative following timedelta's rules. Eg: -5h-30m
    """
    match = TIMEDELTA_PATTERN.match(delta)
    if match:
        months = 0
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        if "months" in parts:
            months = parts.pop("months")
        return add_months(datetime.timedelta(**parts), months)
    return datetime.timedelta(seconds=0)


def parse_iso(value):
    DATE_SEPARATORS = {"-", ":"}
    # date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert upto
    # three times faster than dateutil.

    # valid formats:
    # YYYY-MM-DD
    # YYYY-MM-DD HH:MM
    # YYYY-MM-DDTHH:MM
    # YYYY-MM-DD HH:MM:SS
    # YYYY-MM-DDTHH:MM:SS
    # YYYY-MM-DDTHH:MM:SS
    # 01234567890123456789
    try:
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value
        if isinstance(value, str) and len(value) >= 10:
            if not value[4] in DATE_SEPARATORS or not value[7] in DATE_SEPARATORS:
                return None
            if len(value) == 10:
                # YYYY-MM-DD
                return datetime.datetime(
                    *map(fast_int, [value[:4], value[5:7], value[8:10]])
                )
            if len(value) >= 16:
                if not value[10] in ("T", " ") or not value[13] in DATE_SEPARATORS:
                    return False
                if len(value) >= 19 and value[16] in DATE_SEPARATORS:
                    # YYYY-MM-DDTHH:MM:SS
                    return datetime.datetime(
                        *map(  # type:ignore
                            fast_int,
                            [
                                value[:4],  # YYYY
                                value[5:7],  # MM
                                value[8:10],  # DD
                                value[11:13],  # HH
                                value[14:16],  # MM
                                value[17:19],  # SS
                            ],
                        )
                    )
                else:
                    # YYYY-MM-DDTHH:MM
                    return datetime.datetime(
                        *map(  # type:ignore
                            fast_int,
                            [
                                value[:4],
                                value[5:7],
                                value[8:10],
                                value[11:13],
                                value[14:16],
                            ],
                        )
                    )
        return None
    except (ValueError, TypeError):
        return None


def date_range(
    start_date: Optional[Union[str, datetime.date]],
    end_date: Optional[Union[str, datetime.date]],
):
    """
    An interator over a range of dates
    """
    # if dates aren't provided, use today
    end_date = extract_date(end_date)
    start_date = extract_date(start_date)

    if end_date < start_date:  # type:ignore
        raise ValueError(
            "date_range: end_date must be the same or later than the start_date "
        )

    for n in range(int((end_date - start_date).days) + 1):  # type:ignore
        yield start_date + datetime.timedelta(n)  # type:ignore



if __name__ == "__main__":

    print(add_months(datetime.date(2000,4,30), 6))

    print(parse_delta("12mo"))