import os
import atexit
from mabel.logging import get_logger

can_use_resource_lib = True
try:
    import resource
except ImportError:
    can_use_resource_lib = False
    import psutil  # type:ignore


class ResourceMonitor:
    def __init__(self, frequency=1):
        if not can_use_resource_lib:
            self.process = psutil.Process(os.getpid())
        atexit.register(self.resource_usage)

    def resource_usage(self):
        if can_use_resource_lib:
            memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        else:
            memory_usage = self.process.memory_info()[0]
        get_logger().info({"peak_memory": memory_usage})
