import threading
import os
from time import sleep
#from ..logging import get_logger
import logging

can_use_resource_lib = True
try:
    import resource
except ImportError:
    can_use_resource_lib = False
    import psutil  # type:ignore


class ResourceMonitor():
    def __init__(self, frequency=1):
        self.keep_measuring = True
        self.frequency = frequency
        self.max_memory_usage = 0
        if not can_use_resource_lib:
            self.process = psutil.Process(os.getpid())

    def resource_usage(self):
        while self.keep_measuring:
            if can_use_resource_lib:
                memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            else:
                memory_usage = self.process.memory_info()[0]
            self.max_memory_usage = max(self.max_memory_usage, memory_usage)
            print(F"memory", memory_usage)
            sleep(self.frequency)

        return self.max_memory_usage

    def __del__(self):
        print(F'max usage {self.max_memory_usage}')


monitor = ResourceMonitor()
resource_thread = threading.Thread(target=monitor.resource_usage)
resource_thread.name = 'mabel-resource-monitor'
resource_thread.daemon = True
resource_thread.start()