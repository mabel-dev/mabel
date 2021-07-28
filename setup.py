from setuptools import setup, find_packages  # type:ignore

with open("mabel/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="mabelbeta",
    version=__version__,
    description="Python Data Libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="Joocer",
    author="joocer",
    author_email="justin.joyce@joocer.com",
    packages=find_packages(include=["mabel", "mabel.*"]),
    url="https://github.com/mabel-dev/mabel/",
    install_requires=required,
)
