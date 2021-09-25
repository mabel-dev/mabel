"""
Multiprocessing is not faster in benchmarks, this is being retained but will need
to be manually enabled.

When a reliable use case for multiprocessing is identified it may be included into the
automatic running of the data accesses.
"""
import os
import time
from queue import Empty
import multiprocessing
import logging


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600


def _inner_process(func, source_queue, reply_queue):  # pragma: no cover

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    while source != TERMINATE_SIGNAL:
        # non blocking wait - this isn't thread aware in that it can trivially
        # have race conditions, but it will apply a simple back-off so we're
        # not exhausting memory when we know we should wait
        while reply_queue.full():
            time.sleep(1)
        with multiprocessing.Lock():
            # the empty list here is where the list of indicies should go
            reply_queue.put([d.as_dict() for d in [*func(source, [])]], timeout=30)
        source = None
        while source is None:
            try:
                source = source_queue.get(timeout=1)
            except Empty:  # pragma: no cover
                source = None


def processed_reader(func, items_to_read, support_files):  # pragma: no cover

    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    process_pool = []

    # determin the number of CPUs we're going to use:
    # - less than or equal to the number of files to read
    # - half of the CPUs, unless there's 2, then use both
    slots = max(min(len(items_to_read), multiprocessing.cpu_count() // 2), 2)
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
