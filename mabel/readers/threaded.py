import queue, threading, sys


def threaded_reader(project=None, bucket=None, blobs=[], max_threads=4, chunk_size=16*1024*1024):
    """
    Speed up reading sets of files - such as multiple days worth
    of log-per-day files.
    
    If you care about the order of the records, don't use this.
    
    Each file is in it's own thread, so reading a single file 
    wouldn't benefit from this approach.
    """
    
    thread_pool = []
    
    def thread_process():
        """
        The process inside the threads.
        
        1) Get any files off the file queue
        2) Read the file in chunks - helps with parallelizing
        3) Put the chunk onto a reply queue
        """
        file = file_queue.get() # this will wait until there's a record
        while file:
            #file_reader = read_file(file) #testing on non-gcs environment
            file_reader = gcs.blob_reader(project, bucket, file, chunk_size=chunk_size)
            for chunk in generator_chunker(file_reader, 100):
                reply_queue.put(chunk)
            if file_queue.empty():
                sys.exit()
            else:
                file = file_queue.get()
        sys.exit()

    # establish the queues
    file_queue = queue.SimpleQueue()
    reply_queue = queue.SimpleQueue()

    # scale the number of threads, if we have more than the number
    # of files we're reading, will have threads that never complete
    t = len(blobs)
    if t > max_threads:
        t = max_threads
    # set a hard limit
    if t > 8: 
        t = 8

    # start the threads
    for _ in range(t):
        thread = threading.Thread(target=thread_process)
        thread.daemon = True
        thread.start()
        thread_pool.append(thread)

    # put the files in the file queue
    for blob in blobs:
        file_queue.put_nowait(blob)
    
    # when the threads are all complete and all the records 
    # have been read from the reply queue, we're done
    while any([ t.is_alive() for t in thread_pool ]) or not(reply_queue.empty()):
        records = reply_queue.get()
        yield from records
        
    del file_queue
    del reply_queue
    for t in thread_pool:
        t.kill()
    for t in thread_pool:
        t.join()
