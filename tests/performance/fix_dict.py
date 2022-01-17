import datetime
import csimdjson
import timer
import json


def fix_dict_XXXXXXXXXXXX(obj: dict) -> dict:
    def fix_fields(dt):
        dt_type = type(dt)
        if dt_type in (int, float, bool, str):
            return dt
        if hasattr(dt, "mini"):
            return dt.mini.decode("UTF8")
        if dt_type == dict:
            return str(fix_dict_XXXXXXXXXXXX(dt))
        if dt_type in (list, tuple, set, csimdjson.Array):
            return str([fix_fields(i) for i in dt])
        if dt_type in (datetime.date, datetime.datetime):
            return dt.isoformat()
        if dt_type == bytes:
            return dt.decode("UTF8")
        return str(dt)

    if hasattr(obj, "mini"):
        return obj.mini.decode("UTF8")

    if not isinstance(obj, dict):
        return obj  # type:ignore

    for key in obj.keys():
        obj[key] = fix_fields(obj[key])
    return obj


def fix_dict(obj: dict) -> dict:
    def fix_fields(dt):
        if isinstance(dt, (datetime.date, datetime.datetime)):
            return dt.isoformat()
        if isinstance(dt, (int, float, bool)):
            return dt
        if isinstance(dt, bytes):
            return dt.decode("UTF8")
        if hasattr(dt, "mini"):
            return dt.mini.decode("UTF8")
        if isinstance(dt, dict):
            return {k: fix_fields(v) for k, v in dt.items()}
        if isinstance(dt, (list, tuple, set, csimdjson.Array)):
            return str([fix_fields(i) for i in dt])
        return str(dt)

    if not isinstance(obj, dict):
        return obj  # type:ignore
    return {k: fix_fields(v) for k, v in obj.items()}


d = json.loads(
    """
        {"userid": 612473, "user name": "BBCNews", "user_verified": [true], "followers": 10368450, "tweet": "Sunken chest syndrome: 'I'm being strangled inside' https://t.co/C2EWUFT3Fk", "location": ["London"], "sentiment": -0.25, "timestamp": "2020-01-05T01:16:39"}
        """
)
cycles = 1000000

with timer.Timer("orig"):
    for i in range(cycles):
        fix_dict(d)

with timer.Timer("new "):
    for i in range(cycles):
        fix_dict_XXXXXXXXXXXX(d)
