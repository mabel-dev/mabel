"""
Reader is about 300% faster (8 processes) than the default serial reader.
"""
import multiprocessing
from queue import Empty
from typing import Iterator
import os
from ....logging import get_logger
from multiprocessing import Pool, Manager


def page_dictset(dictset: Iterator[dict], page_size: int) -> Iterator:
    """
    Enables paging through a dictset by returning a page of records at a time.
    Parameters:
        dictset: iterable of dictionaries:
            The dictset to process
        page_size: integer:
            The number of records per page
    Yields:
        dictionary
    """
    chunk: list = []
    for i, record in enumerate(dictset):
        if i % page_size == 0:
            yield chunk
            chunk = [record]
        else:
            chunk.append(record)
    if chunk:
        yield chunk


def _inner_process(reader, reply_queue, parser, source):  # pragma: no cover
    try:
        data = reader.get_records(source)
        data = parser(data)
        for records in page_dictset(data, 256):
            # wait - to save memory we have limited number of slots
            # if we're not read in 30 seconds, abandon
            reply_queue.put(records, timeout=30)
    except Exception as error:
        get_logger().error("Error with multi-processed reader" + error)
    reply_queue.put([], timeout=0)

def processed_reader(items_to_read, reader, parser):  # pragma: no cover

    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    slots = min(4, len(items_to_read), multiprocessing.cpu_count() - 1)
    pool = Pool(slots)
    processes = []
    reply_queue = Manager().Queue(4096)

    for item in items_to_read:
        proc = pool.apply_async(
            _inner_process,
            (reader, reply_queue, parser, item)
        )
        processes.append(proc)
    pool.close()

    try:
        while any({not t.ready() for t in processes}) or not (reply_queue.empty()):
            records = reply_queue.get(timeout=1)
            yield from records
    except Empty:  # nosec
        pass
    finally:
        print("READ")
        pool.terminate()

