import datetime
import re
from typing import Optional
from typing import Union

try:
    import numpy
except ImportError:
    numpy = None

TIMEDELTA_REGEX = (
    r"((?P<days>-?\d+)d)?"
    r"((?P<hours>-?\d+)h)?"
    r"((?P<minutes>-?\d+)m)?"
    r"((?P<seconds>-?\d+)s)?"
)
TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)
SECONDS_PER_HOUR: int = 3600
DATE_SEPARATORS = {"-", ":"}


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


def parse_iso(
    value: Union[str, int, float, datetime.datetime, datetime.date]
) -> Optional[datetime.datetime]:
    """
    Parses an ISO date string into a datetime object, with an emphasis on speed.

    Handles various ISO date formats and converts them to datetime objects. If the input is already a
    datetime object, date object, or a timestamp (int/float), it converts it to a datetime object.
    Non-string inputs or strings not following the ISO format are returned as None.

    Parameters:
        value: Union[str, int, float, datetime.datetime, datetime.date, numpy.datetime64]
            The date value to be parsed.

    Returns:
        datetime.datetime or None: The parsed datetime object, or None if parsing fails.
    """
    # valid formats (not exhaustive):
    #
    #   YYYY-MM-DD                 <- date
    #   YYYY-MM-DD HH:MM           <- date and time, no seconds
    #   YYYY-MM-DDTHH:MM           <- date and time, T separator
    #   YYYY-MM-DD HH:MM:SS        <- date and time with seconds
    #   YYYY-MM-DD HH:MM:SS.mmmm   <- date and time with milliseconds
    #
    # If the last character is a Z, we ignore it.
    # If we have +0000 notation on the end, we ignore it.
    try:
        input_type = type(value)

        if numpy:
            if isinstance(value, numpy.datetime64):
                value = value.astype(datetime.datetime)
                input_type = type(value)

        if input_type == datetime.datetime:
            return value
        if input_type == datetime.date:
            return datetime.datetime.combine(value, datetime.time.min)
        if input_type in (int, float):
            return datetime.datetime.fromtimestamp(value)
        if input_type == str and 10 <= len(value) <= 33:
            if value[-1] == "Z":
                value = value[:-1]
            if "+" in value:
                value = value.split("+")[0]
                if not 10 <= len(value) <= 28:
                    return None
            val_len = len(value)
            if not value[4] in DATE_SEPARATORS or not value[7] in DATE_SEPARATORS:
                return None
            if val_len == 10:
                # YYYY-MM-DD
                return datetime.datetime(*map(int, [value[:4], value[5:7], value[8:10]]))
            if val_len >= 16:
                if not (value[10] in ("T", " ") and value[13] in DATE_SEPARATORS):
                    return None
                if val_len >= 19 and value[16] in DATE_SEPARATORS:
                    # YYYY-MM-DD HH:MM:SS
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
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
                if val_len == 16:
                    # YYYY-MM-DD HH:MM
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


def date_range(
    start_date: Optional[Union[str, datetime.date, datetime.datetime]],
    end_date: Optional[Union[str, datetime.date, datetime.datetime]],
):
    """
    An interator over a range of dates, hour-by-hour
    """
    # if dates aren't provided, use today

    # cover the full day if we've only been given a date
    if type(end_date) == datetime.date:
        end_date = datetime.datetime.combine(end_date, datetime.time.max)
    if isinstance(end_date, str) and len(end_date) == 10:
        end_date += "T23:00:00"

    end_date = parse_iso(end_date)
    if end_date is None:
        end_date = datetime.datetime.utcnow()
    end_date = end_date.replace(minute=0, second=0, microsecond=0)  # type:ignore

    start_date = parse_iso(start_date)
    if start_date is None:
        start_date = datetime.datetime.utcnow()
    start_date = start_date.replace(minute=0, second=0, microsecond=0)  # type:ignore

    if end_date < start_date:  # type:ignore
        raise ValueError("date_range: end_date must be the same or later than the start_date ")

    for n in range(
        int((end_date - start_date).total_seconds() // SECONDS_PER_HOUR)  # type:ignore
        + 1  # type:ignore
    ):  # type:ignore
        yield start_date + datetime.timedelta(hours=n)  # type:ignore
