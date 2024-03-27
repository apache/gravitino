"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import io
import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    content = ""
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


setup(
    name="gravitino",
    version="0.0.1",
    description="project description TBD",
    url="https://github.com/datastrato/gravitino",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="datastrato",
    packages=find_packages(include=["gravitino", ".*"]),
    install_requires=[],
    extras_require={
        "dev": [
            "pytest~=8.0.1",
        ]
    },
)
