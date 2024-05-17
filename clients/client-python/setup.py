"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from setuptools import find_packages, setup


try:
    with open("README.md") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Gravitino Python client"

setup(
    name="gravitino",
    description="Python lib/client for Gravitino",
    version="0.5.0.dev21",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datastrato/gravitino",
    author="datastrato",
    author_email="support@datastrato.com",
    python_requires=">=3.8",
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    install_requires=open("requirements.txt").read(),
    extras_require={
        "dev": open("requirements-dev.txt").read(),
    },
    include_package_data=True,
)
