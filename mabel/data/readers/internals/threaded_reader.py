import queue
import threading
from typing import Iterator


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
    for record in dictset:
        if len(chunk) >= page_size:
            yield chunk
            chunk = [record]
        else:
            chunk.append(record)
    if chunk:
        yield chunk


def threaded_reader(items_to_read: list, blob_list, reader):
    """
    Speed up reading sets of files - such as multiple days worth of log-per-day
    files.

    Each file is read in a separate thread, up to 8 threads, reading a single
    file using this function won't show any improvement.

    NOTE:
        This compromises record ordering to achieve speed increases

    Parameters:
        items_to_read: list of strings
            The name of the blobs to read
        reader: Reader
            The Reader object to perform the reading operations

    Yields:
        dictionary (or string)
    """
    thread_pool = []

    def _thread_process():
        """
        The process inside the threads.

        1) Get any files off the file queue
        2) Read the file in chunks
        3) Put a chunk onto a reply queue
        """
        try:
            source = source_queue.pop(0)
        except IndexError:
            source = None
        while source:
            source_reader = reader._read_blob(source, blob_list)
            for chunk in page_dictset(source_reader, 256):
                reply_queue.put(chunk)  # this will wait until there's a slot
            try:
                source = source_queue.pop(0)
            except IndexError:
                source = None

    source_queue = items_to_read.copy()

    # scale the number of threads, if we have more than the number of files
    # we're reading, will have threads that never complete
    t = min(len(source_queue), reader.thread_count, 8)
    reply_queue: queue.Queue = queue.Queue(t * 8)

    # start the threads
    for _ in range(t):
        thread = threading.Thread(target=_thread_process)
        thread.daemon = True
        thread.start()
        thread_pool.append(thread)

    # when the threads are all complete and all the records have been read from
    # the reply queue, we're done
    while any([t.is_alive() for t in thread_pool]) or not (reply_queue.empty()):
        try:
            # don't wait forever
            records = reply_queue.get(timeout=10)
            yield from records
        except queue.Empty:  # pragma: no cover
            pass  #  most likely reason get being here is a race condition
