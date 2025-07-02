# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from setuptools import find_packages, setup


try:
    with open("README.md") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Apache Gravitino Python client"

setup(
    name="apache-gravitino",
    description="Python lib/client for Apache Gravitino",
    version="0.9.1",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Apache Software Foundation",
    author_email="dev@gravitino.apache.org",
    maintainer="Apache Gravitino Community",
    maintainer_email="dev@gravitino.apache.org",
    license="Apache-2.0",
    url="https://github.com/apache/gravitino",
    python_requires=">=3.8",
    keywords="Data, AI, metadata, catalog",
    packages=find_packages(exclude=["tests*", "scripts*"]),
    project_urls={
        "Homepage": "https://gravitino.apache.org/",
        "Source Code": "https://github.com/apache/gravitino",
        "Documentation": "https://gravitino.apache.org/docs/overview",
        "Bug Tracker": "https://github.com/apache/gravitino/issues",
        "Slack Chat": "https://the-asf.slack.com/archives/C078RESTT19",
    },
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    install_requires=open("requirements.txt").read(),
    extras_require={
        "dev": open("requirements-dev.txt").read(),
    },
    include_package_data=True,
)
