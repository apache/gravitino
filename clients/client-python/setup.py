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

from pathlib import Path

from setuptools import find_packages, setup

CLIENT_PYTHON_ROOT = Path(__file__).resolve().parent


def read_requirements(file_name):
    """Read active requirement lines from a project file."""
    requirements_path = CLIENT_PYTHON_ROOT / file_name
    with requirements_path.open(encoding="utf-8") as requirements_file:
        return [
            line.strip()
            for line in requirements_file
            if line.strip() and not line.lstrip().startswith("#")
        ]


def combine_requirements(*requirement_groups):
    """Combine requirement groups and remove duplicates without reordering."""
    return list(
        dict.fromkeys(
            requirement
            for requirement_group in requirement_groups
            for requirement in requirement_group
        )
    )


gvfs_requirements = read_requirements("requirements-gvfs.txt")
provider_requirement_files = {
    "hdfs": "requirements-hdfs.txt",
    "s3": "requirements-s3.txt",
    "gcs": "requirements-gcs.txt",
    "oss": "requirements-oss.txt",
    "azure": "requirements-azure.txt",
}
provider_requirements = {
    extra_name: combine_requirements(
        gvfs_requirements, read_requirements(requirements_file)
    )
    for extra_name, requirements_file in provider_requirement_files.items()
}
storage_requirements = combine_requirements(
    gvfs_requirements,
    *(
        read_requirements(file_name)
        for file_name in provider_requirement_files.values()
    ),
)


try:
    with open("README.md") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Apache Gravitino Python client"

setup(
    name="apache-gravitino",
    description="Python lib/client for Apache Gravitino",
    version="2.0.0.dev0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Apache Software Foundation",
    author_email="dev@gravitino.apache.org",
    maintainer="Apache Gravitino Community",
    maintainer_email="dev@gravitino.apache.org",
    license="Apache-2.0",
    url="https://github.com/apache/gravitino",
    python_requires=">=3.10",
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
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    install_requires=read_requirements("requirements.txt"),
    extras_require={
        "dev": read_requirements("requirements-dev.txt"),
        "lance": read_requirements("requirements-lance.txt"),
        "gvfs": gvfs_requirements,
        **provider_requirements,
        "storage": storage_requirements,
    },
    include_package_data=True,
)
