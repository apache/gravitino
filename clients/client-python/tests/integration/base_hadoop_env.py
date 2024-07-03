"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import logging
import os
import shutil
import subprocess
import tarfile
from random import randint

import requests

from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException

logger = logging.getLogger(__name__)

HADOOP_VERSION = "2.7.3"
HADOOP_PACK_NAME = f"hadoop-{HADOOP_VERSION}.tar.gz"
HADOOP_DIR_NAME = f"hadoop-{HADOOP_VERSION}"
HADOOP_DOWNLOAD_URL = f"https://archive.apache.org/dist/hadoop/core/hadoop-{HADOOP_VERSION}/{HADOOP_PACK_NAME}"
LOCAL_BASE_DIR = "/tmp/gravitino"
LOCAL_HADOOP_DIR = f"{LOCAL_BASE_DIR}/python_its/hadoop"
LOCAL_DOWNLOAD_TEMP_DIR = f"{LOCAL_HADOOP_DIR}/" + str(randint(1, 10000))


class BaseHadoopEnvironment:

    @classmethod
    def init_hadoop_env(cls):
        # download hadoop pack and unzip
        if not os.path.exists(LOCAL_HADOOP_DIR):
            os.makedirs(LOCAL_HADOOP_DIR)
        if not os.path.exists(LOCAL_DOWNLOAD_TEMP_DIR):
            os.makedirs(LOCAL_DOWNLOAD_TEMP_DIR)
        cls._download_and_unzip_hadoop_pack()
        # configure hadoop env
        cls._configure_hadoop_environment()

    @classmethod
    def clear_hadoop_env(cls):
        try:
            shutil.rmtree(LOCAL_BASE_DIR)
        except OSError as e:
            logger.warning("Failed to delete directory '%s': %s", LOCAL_BASE_DIR, e)

    @classmethod
    def _download_and_unzip_hadoop_pack(cls):
        logger.info("Download and unzip hadoop pack from %s.", HADOOP_DOWNLOAD_URL)
        local_tar_path = f"{LOCAL_DOWNLOAD_TEMP_DIR}/{HADOOP_PACK_NAME}"
        # will download from remote if we do not find the pack locally
        if not os.path.exists(local_tar_path):
            response = requests.get(HADOOP_DOWNLOAD_URL)
            with open(local_tar_path, "wb") as f:
                f.write(response.content)
        # unzip the pack
        try:
            with tarfile.open(local_tar_path) as tar:
                tar.extractall(path=LOCAL_DOWNLOAD_TEMP_DIR)
        except Exception as e:
            raise GravitinoRuntimeException(
                f"Failed to extract file '{local_tar_path}': {e}"
            ) from e
        # move to the hadoop dir
        src_dir = f"{LOCAL_DOWNLOAD_TEMP_DIR}/{HADOOP_DIR_NAME}"
        dst_dir = f"{LOCAL_HADOOP_DIR}/{HADOOP_DIR_NAME}"
        try:
            shutil.move(src_dir, dst_dir)
        except Exception as e:
            raise GravitinoRuntimeException(
                f"Failed to move dir '{src_dir}' to {dst_dir}: {e}"
            ) from e

    @classmethod
    def _configure_hadoop_environment(cls):
        logger.info("Configure hadoop environment.")
        os.putenv("HADOOP_USER_NAME", "datastrato")
        os.putenv("HADOOP_HOME", f"{LOCAL_HADOOP_DIR}/{HADOOP_DIR_NAME}")
        os.putenv(
            "HADOOP_CONF_DIR",
            f"{LOCAL_HADOOP_DIR}/{HADOOP_DIR_NAME}/etc/hadoop",
        )
        hadoop_shell_path = f"{LOCAL_HADOOP_DIR}/{HADOOP_DIR_NAME}/bin/hadoop"
        # get the classpath
        try:
            result = subprocess.run(
                [hadoop_shell_path, "classpath", "--glob"],
                capture_output=True,
                text=True,
                check=True,
            )
            if result.returncode == 0:
                os.putenv("CLASSPATH", str(result.stdout))
            else:
                raise GravitinoRuntimeException(
                    f"Command failed with return code is not 0, stdout: {result.stdout}, stderr:{result.stderr}"
                )
        except subprocess.CalledProcessError as e:
            raise GravitinoRuntimeException(
                f"Command failed with return code {e.returncode}, stderr:{e.stderr}"
            ) from e
