from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize

import os
import glob

with open("mabel/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()


ext_modules = [
    cythonize(
        [
            "mabel/data/internals/group_by.py",
            "mabel/data/internals/dictset.py",
            "mabel/data/internals/expression.py",
            "mabel/data/readers/internals/inline_evaluator.py",
            "mabel/data/readers/internals/parallel_reader.py",
        ]
    )
]


setup(
    name="mabel",
    version=__version__,
    description="Python Data Libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="Joocer",
    author="joocer",
    author_email="justin.joyce@joocer.com",
    packages=find_packages(where="mabel"),
    url="https://github.com/mabel-dev/mabel/",
    install_requires=required,
    ext_modules=ext_modules,
)
