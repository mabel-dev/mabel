"""
Advanced Error Trace Formatting

Adapted From:
https://github.com/willmcgugan/rich/blob/master/rich/traceback.py
Copyright 2020 Will McGugan
MIT Licence

#nodoc - don't add to the documentation wiki
"""
import os.path
import pathlib
import sys
from typing import Dict, List, Optional, Any, Generator
from dataclasses import dataclass, field
from traceback import walk_tb

LINE_LENGTH = 120
BAR = " " * LINE_LENGTH


@dataclass
class Frame:
    filename: str
    lineno: int
    name: str
    line: str = ""
    locals: Optional[Dict[str, Any]] = None


@dataclass
class Stack:
    exc_type: str
    exc_value: str
    is_cause: bool = False
    frames: List[Frame] = field(default_factory=list)


def wrap_text(text, line_len):
    from textwrap import fill

    def _inner(text):
        for line in text.splitlines():
            yield fill(line, line_len)

    return "\n".join(list(_inner(text)))


def bar_label(label):
    if len(label) > 0:
        center = ("**" + label + "**").center(LINE_LENGTH)
        return center.replace(" ", "=").replace("*", " ")
    else:
        return "-" * LINE_LENGTH


def _build_error_stack():
    exc_type, exc_value, traceback = sys.exc_info()

    # if there was no error, just return
    if exc_type is None:
        return []

    stacks = []
    is_cause = False

    while True:
        stack = Stack(
            exc_type=str(exc_type.__name__), exc_value=str(exc_value), is_cause=is_cause
        )

        stacks.append(stack)
        append = stack.frames.append

        for frame_summary, line_no in walk_tb(traceback):
            filename = frame_summary.f_code.co_filename
            filename = os.path.abspath(filename) if filename else "?"
            frame = Frame(
                filename=filename,
                lineno=line_no,
                name=frame_summary.f_code.co_name,
                locals={key: value for key, value in frame_summary.f_locals.items()},
            )
            append(frame)

        cause = getattr(exc_value, "__cause__", None)
        if cause and cause.__traceback__:
            exc_type = cause.__class__
            exc_value = cause
            traceback = cause.__traceback__
            if traceback:
                is_cause = True
                continue

        cause = exc_value.__context__
        if (
            cause
            and cause.__traceback__
            and not getattr(exc_value, "__suppress_context__", False)
        ):
            exc_type = cause.__class__
            exc_value = cause
            traceback = cause.__traceback__
            if traceback:
                is_cause = False
                continue
        # No cover, code is reached but coverage doesn't recognize it.
        break
    return stacks


def _render_locals(locals):
    if locals:
        yield bar_label("locals")
        max_label_len = max([len(key) for key, value in locals.items()])
        for key, value in locals.items():
            yield f"  {key:<{max_label_len}} : {value}"


def _read_from_code(filename: str, line: int, extend_by: int) -> Generator:
    try:
        with open(filename, "rt", encoding="utf-8") as code_file:
            code = code_file.read()

        lines = code.splitlines()
        start_line = max(line - extend_by, 0)
        end_line = min(line + extend_by + 1, len(lines) + 1)

        path = pathlib.Path(filename)

        yield bar_label(path.stem + path.suffix)
        for line_number in range(start_line, end_line):
            prefix = ">" if line_number == line else " "
            yield f"{prefix}{line_number:4d} | {lines[line_number-1]}"
            line_number += 1
    except:
        return ""


def _render_error_stack():
    stack = _build_error_stack()
    for item in stack:
        for frame in item.frames:
            yield f"{frame.filename}:{frame.lineno} in {frame.name}()"
            if frame.filename.startswith("<"):
                continue
            yield from _render_locals(frame.locals)
            yield from _read_from_code(
                filename=frame.filename, line=frame.lineno, extend_by=3
            )

            yield bar_label("")
            yield ""


def render_error_stack():
    s = [wrap_text(line, LINE_LENGTH) for line in _render_error_stack()]
    return "\n".join(s)
