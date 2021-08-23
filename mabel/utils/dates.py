import re
import datetime


TIMEDELTA_REGEX = (
    r"((?P<days>-?\d+)d)?"
    r"((?P<hours>-?\d+)h)?"
    r"((?P<minutes>-?\d+)m)?"
    r"((?P<seconds>-?\d+)s)?"
)
TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)

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
    DATE_SEPARATORS = {"-", "\\", "/", ":"}
    # date validation at speed is hard, dateutil is great but really slow, this is fast
    # but error-prone. It assumes it is a date or it really nothing like a date.
    # Making that assumption - and accepting the consequences - we can convert upto
    # three times faster than dateutil.
    try:
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value
        if isinstance(value, str):
            if not value[4] in DATE_SEPARATORS or not value[7] in DATE_SEPARATORS:
                return None
            if len(value) == 10:
                # YYYY-MM-DD
                return datetime.date(*map(int, [value[:4], value[5:7], value[8:10]]))
            else:
                if not value[10] == "T" or not value[13] in DATE_SEPARATORS:
                    return False
                # YYYY-MM-DDTHH:MM
                return datetime.datetime(
                    *map(  # type:ignore
                        int,
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
