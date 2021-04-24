# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully and fall back to the actual os
# environment.
try:
    from dotenv import load_dotenv   # type:ignore
    from pathlib import Path
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    pass

import os
os.system("")  # nosec - added as part of formatting log messages

from .version import __version__

from .flows.flow import Flow
from .data.readers.reader import Reader
from .flows.internals.base_operator import BaseOperator
from .flows.internals.decorators import operatify
