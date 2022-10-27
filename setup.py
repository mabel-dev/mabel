from setuptools import setup, find_packages
from Cython.Build import cythonize

with open("mabel/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open("mabel.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

ext_modules = cythonize(
    [
        "mabel/data/internals/group_by.py",
        "mabel/data/internals/dictset.py",
        "mabel/data/internals/expression.py",
        "mabel/data/readers/internals/inline_evaluator.py",
        "mabel/data/readers/internals/parallel_reader.py",
    ]
)

setup(
    name="mabel",
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
    ext_modules=ext_modules,
)
