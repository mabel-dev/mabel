from setuptools import setup, find_packages  # type:ignore

with open("mabel/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

setup(
    name="mabel",
    version=__version__,
    description="Python Data Libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="Joocer",
    packages=find_packages(include=["mabel", "mabel.*"]),
    url="https://github.com/mabel-dev/mabel/",
    install_requires=["ujson", "python-dateutil", "zstandard", "mmh3", "pydantic"],
)
