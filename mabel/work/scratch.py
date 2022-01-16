import sys
import time
from  multiprocessing import Process, Queue
 
def worker(q):
    for task_nbr in range(10000000):
        message = q.get()
    import logging
    logging.error("DONE")
    sys.exit(1)
  
def main():
    send_q = Queue()
    Process(target=worker, args=(send_q,)).start()
    for num in range(10000000):
        send_q.put("MESSAGE")
 
if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = 10000000 / duration
 
    print ("Duration: %s" % duration)
    print ("Messages Per Second: %s" % msg_per_sec)