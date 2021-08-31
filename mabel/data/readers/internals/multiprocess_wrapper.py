"""
The naive solution for using multiprocessing loads the entire dataset into memory,
this is okay for small datasets, but quickly causes halts and crashes. This helper
routine gets around this by throttling the worker processes - a new piece of work 
is added as a piece is read off the queue - meaning each process only has one
file to load at a time.
"""
import os
from queue import Empty
import time
import multiprocessing


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600


def _inner_process(flag, func, source_queue, reply_queue):  # pragma: no cover

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = None

    while source and source != TERMINATE_SIGNAL:
        reply_queue.put(func(source))
        try:
            source = source_queue.get(timeout=1)
        except Empty:  # pragma: no cover
            source = None

    flag.value = TERMINATE_SIGNAL


def processed_reader(func, items_to_read):  # pragma: no cover

    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    process_pool = []

    # limit the number of slots
    slots = min(len(items_to_read), multiprocessing.cpu_count() - 1)
    reply_queue = multiprocessing.Queue()

    send_queue = multiprocessing.Queue()
    for item_index in range(slots):
        if item_index < len(items_to_read):
            send_queue.put(items_to_read[item_index])

    for i in range(slots):
        flag = multiprocessing.Value("i", 1 - TERMINATE_SIGNAL)
        process = multiprocessing.Process(
            target=_inner_process,
            args=(flag, func, send_queue, reply_queue),
        )
        process.daemon = True
        process.start()
        process_pool.append(flag)

    process_start_time = time.time()
    item_index = slots

    # if we're searching for a rare term, it will go seconds without returning
    # any results, but we shouldn't wait forever
    while any({t.value == 1 - TERMINATE_SIGNAL for t in process_pool}) or not (
        reply_queue.empty()
    ):
        try:
            records = reply_queue.get(timeout=1)
            yield from records

            if item_index < len(items_to_read):
                send_queue.put_nowait(items_to_read[item_index])
                item_index += 1
            else:
                send_queue.put_nowait(TERMINATE_SIGNAL)

        except Empty:  # nosec
            if time.time() - process_start_time > MAXIMUM_SECONDS_PROCESSES_CAN_RUN:
                print(
                    f"Sending TERMINATE to long running multi-processed processes after {MAXIMUM_SECONDS_PROCESSES_CAN_RUN} seconds total run time"
                )
                for flag in process_pool:
                    flag.value = TERMINATE_SIGNAL
