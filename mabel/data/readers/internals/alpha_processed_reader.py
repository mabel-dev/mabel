"""
Reader is about 300% faster (8 processes) than the default serial reader.

Problems
- The .limit method causes this reader to never end ()
"""
import multiprocessing
import os
import time
from ...formats import dictset
from ....logging import get_logger


MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600  # 5 minutes
TERMINATE_SIGNAL = -1


def _inner_process(
    flag, reader, source_queue, reply_queue, parser, where
):  # pragma: no cover

    try:
        source = source_queue.get(timeout=0.1)
    except Exception:  # pragma: no cover
        source = None

    while source is not None and flag.value != TERMINATE_SIGNAL:
        data = reader.get_records(source)
        data = parser(data)
        if where is not None:
            data = filter(where, data)
        for chunk in dictset.page_dictset(data, 256):
            # wait - to save memory we have limited number of slots
            reply_queue.put(chunk, timeout=60)
        try:
            source = source_queue.get(timeout=0.1)
        except Exception:  # pragma: no cover
            source = None

    flag.value = TERMINATE_SIGNAL
    get_logger().debug("Terminating background process")


def processed_reader(items_to_read, reader, parser, where):  # pragma: no cover

    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    process_pool = []

    send_queue = multiprocessing.Queue()
    for item in items_to_read:
        send_queue.put(item)

    # limit the number of slots
    slots = min(8, len(items_to_read), multiprocessing.cpu_count())
    reply_queue = multiprocessing.Queue(slots * 16)

    for _ in range(slots):
        flag = multiprocessing.Value("i", 1 - TERMINATE_SIGNAL)
        process = multiprocessing.Process(
            target=_inner_process,
            args=(flag, reader, send_queue, reply_queue, parser, where),
        )
        process.daemon = True
        process.start()
        process_pool.append(flag)

    process_pool = set(process_pool)

    process_start_time = time.time()

    # if we're searching for a rare term, it will go seconds without returning
    # any results, but we shouldn't wait forever
    while any({t.value == 1 - TERMINATE_SIGNAL for t in process_pool}) or not (
        reply_queue.empty()
    ):
        try:
            records = reply_queue.get(timeout=1)
            yield from records
        except:  # nosec
            if time.time() - process_start_time > MAXIMUM_SECONDS_PROCESSES_CAN_RUN:
                get_logger().debug(
                    f"Sending TERMINATE to long running multi-processed processes after {MAXIMUM_SECONDS_PROCESSES_CAN_RUN} seconds total run time"
                )
                for flag in process_pool:
                    flag.value = TERMINATE_SIGNAL
