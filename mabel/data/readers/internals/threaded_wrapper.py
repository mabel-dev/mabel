"""

"""
import time
from queue import Empty
import threading
import logging
from queue import SimpleQueue


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600


def _inner_process(func, source_queue, reply_queue):  # pragma: no cover

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    while source != TERMINATE_SIGNAL:
        reply_queue.put(
            func(source, []),
            timeout=30,
        )
        source = None
        while source is None:
            try:
                source = source_queue.get(timeout=1)
            except Empty:  # pragma: no cover
                source = TERMINATE_SIGNAL


def processed_reader(func, items_to_read, support_files):  # pragma: no cover

    process_pool = []

    slots = 4
    reply_queue = SimpleQueue()

    send_queue = SimpleQueue()
    for item_index in range(slots):
        if item_index < len(items_to_read):
            send_queue.put(items_to_read[item_index])

    for i in range(slots):
        process = threading.Thread(
            target=_inner_process,
            args=(func, send_queue, reply_queue),
        )
        process.daemon = True
        process.start()
        process_pool.append(process)

    process_start_time = time.time()
    item_index = slots

    while any({p.is_alive() for p in process_pool}):
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
                logging.error(
                    f"Sending TERMINATE to long running multi-processed processes after {MAXIMUM_SECONDS_PROCESSES_CAN_RUN} seconds total run time"
                )
                break
        except GeneratorExit:
            logging.error("GENERATOR EXIT DETECTED")
            break

    for process in process_pool:
        process.join()
