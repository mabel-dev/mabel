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

from .version import __version__
