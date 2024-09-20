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

import logging
import os
import shutil
import subprocess
import tarfile

from gravitino.exceptions.base import GravitinoRuntimeException

logger = logging.getLogger(__name__)

HADOOP_VERSION = os.environ.get("HADOOP_VERSION")
PYTHON_BUILD_PATH = os.environ.get("PYTHON_BUILD_PATH")


class BaseHadoopEnvironment:

    @classmethod
    def init_hadoop_env(cls):
        cls._unzip_hadoop_pack()
        # configure hadoop env
        cls._configure_hadoop_environment()

    @classmethod
    def clear_hadoop_env(cls):
        try:
            shutil.rmtree(f"{PYTHON_BUILD_PATH}/hadoop")
        except Exception as e:
            raise GravitinoRuntimeException(
                f"Failed to delete dir '{PYTHON_BUILD_PATH}/hadoop': {e}"
            ) from e

    @classmethod
    def _unzip_hadoop_pack(cls):
        hadoop_pack = f"{PYTHON_BUILD_PATH}/tmp/hadoop-{HADOOP_VERSION}.tar.gz"
        unzip_dir = f"{PYTHON_BUILD_PATH}/hadoop"
        logger.info("Unzip hadoop pack from %s.", hadoop_pack)
        # unzip the pack
        if os.path.exists(unzip_dir):
            try:
                shutil.rmtree(unzip_dir)
            except Exception as e:
                raise GravitinoRuntimeException(
                    f"Failed to delete dir '{unzip_dir}': {e}"
                ) from e
        try:
            with tarfile.open(hadoop_pack) as tar:
                tar.extractall(path=unzip_dir)
        except Exception as e:
            raise GravitinoRuntimeException(
                f"Failed to extract file '{hadoop_pack}': {e}"
            ) from e

    @classmethod
    def _configure_hadoop_environment(cls):
        logger.info("Configure hadoop environment.")
        os.putenv("HADOOP_USER_NAME", "anonymous")
        os.putenv("HADOOP_HOME", f"{PYTHON_BUILD_PATH}/hadoop/hadoop-{HADOOP_VERSION}")
        os.putenv(
            "HADOOP_CONF_DIR",
            f"{PYTHON_BUILD_PATH}/hadoop/hadoop-{HADOOP_VERSION}/etc/hadoop",
        )
        hadoop_shell_path = (
            f"{PYTHON_BUILD_PATH}/hadoop/hadoop-{HADOOP_VERSION}/bin/hadoop"
        )
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
