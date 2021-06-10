# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully and fall back to the actual os
# environment.
try:
    from dotenv import load_dotenv  # type:ignore
    from pathlib import Path

    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path)
except ImportError:  # pragma: no cover
    pass

import os
from .version import __version__

if os.environ.get("RESOURCE_MONITORING", False):  # pragma: no cover
    from .utils.resource_monitoring import ResourceMonitor

from .flows.flow import Flow
from .flows.internals.base_operator import BaseOperator
from .flows.internals.decorators import operator
from .data.readers.reader import Reader
