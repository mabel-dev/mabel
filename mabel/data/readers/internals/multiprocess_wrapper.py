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
import logging


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600


def _inner_process(func, source_queue, reply_queue):  # pragma: no cover

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    while source != TERMINATE_SIGNAL:  # and flag.value != TERMINATE_SIGNAL:
        # no blocking wait - this isn't thread aware in that it can trivially
        # have race conditions, but it will apply a simple back-off so we're
        # not exhausting memory when we know we should wait
        while reply_queue.full():
            time.sleep(1)
        with multiprocessing.Lock():
            reply_queue.put(func(source), timeout=30)
        source = None
        while source is None:
            try:
                source = source_queue.get(timeout=1)
            except Empty:  # pragma: no cover
                source = None


def processed_reader(func, items_to_read):  # pragma: no cover

    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    process_pool = []

    # limit the number of slots
    slots = min(len(items_to_read), multiprocessing.cpu_count() - 1)
    reply_queue = multiprocessing.Queue(slots)

    send_queue = multiprocessing.Queue()
    with multiprocessing.Lock():
        for item_index in range(slots):
            if item_index < len(items_to_read):
                send_queue.put(items_to_read[item_index])

    for i in range(slots):
        process = multiprocessing.Process(
            target=_inner_process,
            args=(func, send_queue, reply_queue),
        )
        process.daemon = True
        process.start()
        process_pool.append(process)

    process_start_time = time.time()
    item_index = slots

    while (
        any({p.is_alive() for p in process_pool})
        or not reply_queue.empty()
        or not send_queue.empty()
    ):
        try:
            records = reply_queue.get(timeout=1)
            yield from records

            if item_index < len(items_to_read):
                with multiprocessing.Lock():
                    send_queue.put_nowait(items_to_read[item_index])
                item_index += 1
            else:
                with multiprocessing.Lock():
                    send_queue.put_nowait(TERMINATE_SIGNAL)

        except Empty:  # nosec
            if time.time() - process_start_time > MAXIMUM_SECONDS_PROCESSES_CAN_RUN:
                logging.error(
                    f"Sending TERMINATE to long running multi-processed processes after {MAXIMUM_SECONDS_PROCESSES_CAN_RUN} seconds total run time"
                )
                break
        except GeneratorExit:
            logging.error("GENERATOR EXIT DETECTED")
            break

    reply_queue.close()
    send_queue.close()
    reply_queue.join_thread()
    send_queue.join_thread()
    for process in process_pool:
        process.join()
