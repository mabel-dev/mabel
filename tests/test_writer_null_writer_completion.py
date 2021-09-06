import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.adapters.disk.disk_reader import DiskReader
from mabel.operators.reader_operator import ReaderOperator
from mabel.adapters.null import NullWriter
from mabel.data import BatchWriter
from rich import traceback
from mabel import BaseOperator
from mabel.operators import EndOperator
from mabel.operators.null import NullBatchWriterOperator


class FailOnT800Operator(BaseOperator):
    def execute(self, data: dict, context: dict):
        if data.get("name") == "T-800":
            raise Exception("Terminator Found")
        return data, context


class FeedMeRobotsOperator(BaseOperator):
    def execute(self, data: dict, context: dict):
        for robot in ROBOTS:
            yield robot, context


class WasteAMillisecondOperator(BaseOperator):
    def execute(self, data: dict, context: dict):
        time.sleep(1)
        return data, context


traceback.install()

ROBOTS = [
    {"name": "K-2SO"},
    {"name": "Jonny-5"},
    {"name": "Astro Boy"},
    {"name": "Vision"},
    {"name": "Ava"},
    {"name": "Baymax"},
    {"name": "WALL-E"},
    {"name": "T-800"},
]


def test_null_writer():
    w = BatchWriter(inner_writer=NullWriter, dataset="nowhere")
    for bot in ROBOTS:
        w.append(bot)
    assert w.finalize(has_failure=True) == -1


def test_timing_out_flow(caplog):
    flow = (
        ReaderOperator(
            time_out=0.5,
            inner_reader=DiskReader,
            dataset="tests/data/formats/jsonl",
            raw_path=True,
        )
        >> WasteAMillisecondOperator()
        >> NullBatchWriterOperator(dataset="NOWHERE")
        >> EndOperator()
    )

    try:
        with flow as runner:
            runner(None, None, 0)
    except Exception as err:
        pass

    if caplog is None:
        print("Test must be run in pytest")
        return

    frame_written = False
    frame_skipped = False

    for log_name, log_level, log_text in caplog.record_tuples:
        if "Frame completion file" in log_text:
            frame_written = True
        if "Error found in the stack, not marking frame as complete." in log_text:
            frame_skipped = True

    assert not frame_written
    assert frame_skipped


def test_terminating_flow(caplog):
    flow = (
        FeedMeRobotsOperator()
        >> FailOnT800Operator()
        >> NullBatchWriterOperator(dataset="NOWHERE")
        >> EndOperator()
    )
    try:
        with flow as runner:
            runner(None, None, 0)
    except:
        pass

    if caplog is None:
        print("Test must be run in pytest")
        return

    frame_written = False
    frame_skipped = False

    for log_name, log_level, log_text in caplog.record_tuples:
        if "Frame completion file" in log_text:
            frame_written = True
        if "Error found in the stack, not marking frame as complete." in log_text:
            frame_skipped = True

    assert not frame_written
    assert frame_skipped


if __name__ == "__main__":  # pragma: no cover
    test_null_writer()
    test_terminating_flow(None)
    test_timing_out_flow(None)

    print("okay")
