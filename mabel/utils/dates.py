import re
import datetime
from fastnumbers import fast_int
from typing import Optional, Union
from string import Formatter

TIMEDELTA_REGEX = (
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


# based on:
# https://gist.github.com/santiagobasulto/698f0ff660968200f873a2f9d1c4113c#file-parse_timedeltas-py
def parse_delta(delta: str) -> datetime.timedelta:
    """
    Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.

    Delta includes:
    * Xd days
    * Xh hours
    * Xm minutes
    * Xs seconds

    Values can be negative following timedelta's rules. Eg: -5h-30m
    """
    match = TIMEDELTA_PATTERN.match(delta)
    if match:
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        return datetime.timedelta(**parts)
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
                if value[16] in DATE_SEPARATORS:
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


# https://stackoverflow.com/questions/538666/format-timedelta-to-string/42320260#42320260
def format_delta(tdelta, fmt="{D:02}d {H:02}h {M:02}m {S:02}s", inputtype="timedelta"):
    """Convert a datetime.timedelta object or a regular number to a custom-
    formatted string, just like the stftime() method does for datetime.datetime
    objects.

    The fmt argument allows custom formatting to be specified.  Fields can
    include seconds, minutes, hours, days, and weeks.  Each field is optional.

    Some examples:
        '{D:02}d {H:02}h {M:02}m {S:02}s' --> '05d 08h 04m 02s' (default)
        '{W}w {D}d {H}:{M:02}:{S:02}'     --> '4w 5d 8:04:02'
        '{D:2}d {H:2}:{M:02}:{S:02}'      --> ' 5d  8:04:02'
        '{H}h {S}s'                       --> '72h 800s'

    The inputtype argument allows tdelta to be a regular number instead of the
    default, which is a datetime.timedelta object.  Valid inputtype strings:
        's', 'seconds',
        'm', 'minutes',
        'h', 'hours',
        'd', 'days',
        'w', 'weeks'
    """

    # Convert tdelta to integer seconds.
    if inputtype == "timedelta":
        remainder = int(tdelta.total_seconds())
    elif inputtype in ["s", "seconds"]:
        remainder = int(tdelta)
    elif inputtype in ["m", "minutes"]:
        remainder = int(tdelta) * 60
    elif inputtype in ["h", "hours"]:
        remainder = int(tdelta) * 3600
    elif inputtype in ["d", "days"]:
        remainder = int(tdelta) * 86400
    elif inputtype in ["w", "weeks"]:
        remainder = int(tdelta) * 604800

    f = Formatter()
    desired_fields = [field_tuple[1] for field_tuple in f.parse(fmt)]
    possible_fields = ("W", "D", "H", "M", "S")
    constants = {"W": 604800, "D": 86400, "H": 3600, "M": 60, "S": 1}
    values = {}
    for field in possible_fields:
        if field in desired_fields and field in constants:
            values[field], remainder = divmod(remainder, constants[field])
    return f.format(fmt, **values)
