# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully and fall back to the actual os
# environment.

try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

from pathlib import Path

env_path = Path(".") / ".env"

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if env_path.exists() and (dotenv is None):  # pragma: no cover  # nosemgrep
    # using logger here will tie us in knots
    print("`.env` file exists but `dotEnv` not installed.")
elif dotenv is not None:  # pragma: no cover
    dotenv.load_dotenv(dotenv_path=env_path)

import os
from .version import __version__

if os.environ.get("RESOURCE_MONITORING", False):  # pragma: no cover
    from .utils.resource_monitoring import ResourceMonitor

from .flows.flow import Flow
from .flows.internals.base_operator import BaseOperator
from .flows.internals.decorators import operator
from .data.readers.reader import Reader
from .data.readers.internals.sql_reader import SqlReader
from .data.writers.writer import Writer
from .data.internals.dictset import DictSet
