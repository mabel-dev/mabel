import os
import sys
from datetime import timedelta

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.utils.dates import parse_delta


def test_day_deltas():
    assert parse_delta("3d") == timedelta(days=3)
    assert parse_delta("-3d") == timedelta(days=-3)
    assert parse_delta("-37D") == timedelta(days=-37)


def test_hours_deltas():
    assert parse_delta("18h") == timedelta(hours=18)
    assert parse_delta("-5h") == timedelta(hours=-5)
    assert parse_delta("11H") == timedelta(hours=11)


def test_minute_deltas():
    assert parse_delta("129m") == timedelta(minutes=129)
    assert parse_delta("-68m") == timedelta(minutes=-68)
    assert parse_delta("12M") == timedelta(minutes=12)


def test_second_deltas():
    assert parse_delta("300s") == timedelta(seconds=300)
    assert parse_delta("-5s") == timedelta(seconds=-5)
    assert parse_delta("23S") == timedelta(seconds=23)


def test_combined_deltas():
    assert parse_delta("3d5h") == timedelta(days=3, hours=5)
    assert parse_delta("-3d-5h") == timedelta(days=-3, hours=-5)
    assert parse_delta("13d4h19m") == timedelta(days=13, hours=4, minutes=19)

    assert parse_delta("13d19m") == timedelta(days=13, minutes=19)
    assert parse_delta("-13d-19m") == timedelta(days=-13, minutes=-19)
    assert parse_delta("-13d19m") == timedelta(days=-13, minutes=19)
    assert parse_delta("13d-19m") == timedelta(days=13, minutes=-19)

    assert parse_delta("4h19m") == timedelta(hours=4, minutes=19)
    assert parse_delta("-4h-19m") == timedelta(hours=-4, minutes=-19)
    assert parse_delta("-4h19m") == timedelta(hours=-4, minutes=19)
    assert parse_delta("4h-19m") == timedelta(hours=4, minutes=-19)


def test_invalid_scenarios():
    assert parse_delta("") == timedelta(seconds=0), parse_delta("")
    assert parse_delta("five seconds") == timedelta(seconds=-0), parse_delta("five seconds")
    assert parse_delta("tjyhrsdy") == timedelta(seconds=0), parse_delta("tjyhrsdy")
    assert parse_delta("1m") == timedelta(seconds=60), parse_delta("1m")
    assert parse_delta("6m 1x") == timedelta(seconds=360), parse_delta("6m 1x")


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
