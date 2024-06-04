"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import os
import random
import shutil
import string
import tarfile

import requests

from tests.integration.integration_test_env import IntegrationTestEnv


def generate_unique_random_string(length):
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.sample(characters, length))
    return random_string


class TestGVFS(IntegrationTestEnv):
    _hadoop_version = "3.1.0"
    _hadoop_distribution_url = (
        f"https://archive.apache.org/dist/hadoop/common/hadoop-{_hadoop_version}"
        f"/hadoop-{_hadoop_version}.tar.gz"
    )
    _local_base_path = "/tmp/gravitino/hadoop"
    _local_test_path = f"{_local_base_path}/{generate_unique_random_string(10)}"
    _local_save_path = f"{_local_test_path}/hadoop-{_hadoop_version}.tar.gz"
    _local_extract_path = f"{_local_test_path}/hadoop-{_hadoop_version}"

    def setUp(self):
        if not os.path.exists(self._local_test_path):
            os.makedirs(self._local_test_path, exist_ok=True)
        # download hadoop distribution
        response = requests.get(self._hadoop_distribution_url)
        with open(self._local_save_path, "wb") as file:
            file.write(response.content)
        os.makedirs(self._local_extract_path, exist_ok=True)
        # extract hadoop distribution package
        with tarfile.open(self._local_save_path, "r:gz") as tar:
            tar.extractall(path=self._local_extract_path)
        # set environment variables
        os.environ["HADOOP_HOME"] = self._local_extract_path
        os.environ["HADOOP_CONF_DIR"] = f"{self._local_extract_path}/etc/hadoop"
        os.environ["CLASSPATH"] = "$HADOOP_HOME/bin/hdfs classpath --glob"
        os.environ["PATH"] = f"{os.environ['PATH']}:$HADOOP_HOME/bin"

    def tearDown(self):
        if os.path.exists(self._local_base_path):
            shutil.rmtree(self._local_base_path)

    def test_hadoop_env(self):
        # test hadoop environment variables
        hadoop_home = os.environ["HADOOP_HOME"]
        self.assertTrue(self._local_extract_path, hadoop_home)
        self.assertTrue(os.path.exists(self._local_extract_path))
