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
import unittest
import subprocess
import time
import sys
import shutil

import requests

from gravitino.exceptions.base import GravitinoRuntimeException
from tests.integration.config import Config

logger = logging.getLogger(__name__)


def get_gravitino_server_version(**kwargs):
    try:
        response = requests.get(
            "http://localhost:8090/api/version",
            timeout=Config.REQUEST["TIMEOUT"],
            **kwargs,
        )
        response.raise_for_status()  # raise an exception for bad status codes
        response.close()
        return True
    except requests.exceptions.RequestException as e:
        logger.warning("Failed to access the Gravitino server: %s", e)
        return False


def check_gravitino_server_status(**kwargs) -> bool:
    gravitino_server_running = False
    for i in range(5):
        logger.info("Monitoring Gravitino server status. Attempt %s", i + 1)
        if get_gravitino_server_version(**kwargs):
            logger.debug("Gravitino Server is running")
            gravitino_server_running = True
            break
        else:
            logger.debug("Gravitino Server is not running")
            time.sleep(1)
    return gravitino_server_running


class IntegrationTestEnv(unittest.TestCase):
    """Provide real test environment for the Gravitino Server"""

    gravitino_startup_script = None

    @classmethod
    def setUpClass(cls):
        if (
            os.environ.get("START_EXTERNAL_GRAVITINO") is not None
            and os.environ.get("START_EXTERNAL_GRAVITINO").lower() == "true"
        ):
            # Maybe Gravitino server already startup by Gradle test command or developer manual startup.
            if not check_gravitino_server_status():
                logger.error("ERROR: Can't find online Gravitino server!")
            return

        cls._get_gravitino_home()
        cls.gravitino_startup_script = os.path.join(
            cls.gravitino_home, "bin/gravitino.sh"
        )
        if not os.path.exists(cls.gravitino_startup_script):
            logger.error(
                "Can't find Gravitino startup script: %s, "
                "Please execute `./gradlew compileDistribution -x test` in the Gravitino project root "
                "directory.",
                cls.gravitino_startup_script,
            )
            sys.exit(0)

        # remove data dir under gravitino_home
        data_dir = os.path.join(cls.gravitino_home, "data")
        if os.path.exists(data_dir):
            logger.info("Remove Gravitino data directory: %s", data_dir)
            shutil.rmtree(data_dir)

        logger.info("Starting integration test environment...")

        # Start Gravitino Server
        result = subprocess.run(
            [cls.gravitino_startup_script, "start"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        if not check_gravitino_server_status():
            logger.error("ERROR: Can't start Gravitino server!")
            sys.exit(0)

    @classmethod
    def tearDownClass(cls):
        if (
            os.environ.get("START_EXTERNAL_GRAVITINO") is not None
            and os.environ.get("START_EXTERNAL_GRAVITINO").lower() == "true"
        ):
            return

        logger.info("Stop integration test environment...")
        result = subprocess.run(
            [cls.gravitino_startup_script, "stop"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        gravitino_server_running = True
        for i in range(5):
            logger.debug("Monitoring Gravitino server status. Attempt %s", i + 1)
            if check_gravitino_server_status():
                logger.debug("Gravitino server still running")
                time.sleep(1)
            else:
                logger.debug("Stop Gravitino server successes!")
                gravitino_server_running = False
                break

        if gravitino_server_running:
            logger.error("Can't stop Gravitino server!")

    @classmethod
    def restart_server(cls):
        logger.info("Restarting Gravitino server...")
        gravitino_home = os.environ.get("GRAVITINO_HOME")
        gravitino_startup_script = os.path.join(gravitino_home, "bin/gravitino.sh")
        if not os.path.exists(gravitino_startup_script):
            raise GravitinoRuntimeException(
                f"Can't find Gravitino startup script: {gravitino_startup_script}, "
                "Please execute `./gradlew compileDistribution -x test` in the Gravitino "
                "project root directory."
            )

        # remove data dir under gravitino_home
        data_dir = os.path.join(gravitino_home, "data")
        if os.path.exists(data_dir):
            logger.info("Remove Gravitino data directory: %s", data_dir)
            shutil.rmtree(data_dir)

        # Restart Gravitino Server
        env_vars = os.environ.copy()
        env_vars["HADOOP_USER_NAME"] = "anonymous"
        result = subprocess.run(
            [gravitino_startup_script, "restart"],
            env=env_vars,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        if not check_gravitino_server_status():
            raise GravitinoRuntimeException("ERROR: Can't start Gravitino server!")

    @classmethod
    def _get_gravitino_home(cls):
        gravitino_home = os.environ.get("GRAVITINO_HOME")
        if gravitino_home is None:
            logger.error(
                "Gravitino Python client integration test must configure `GRAVITINO_HOME`"
            )
            sys.exit(0)

        cls.gravitino_home = gravitino_home

    @classmethod
    def _append_conf(cls, config, conf_path):
        logger.info("Append %s.", conf_path)
        if not os.path.exists(conf_path):
            raise GravitinoRuntimeException(f"Conf file is not found at `{conf_path}`.")

        with open(conf_path, mode="a", encoding="utf-8") as f:
            for key, value in config.items():
                f.write(f"\n{key} = {value}")

    @classmethod
    def _reset_conf(cls, config, conf_path):
        logger.info("Reset %s.", conf_path)
        if not os.path.exists(conf_path):
            raise GravitinoRuntimeException(f"Conf file is not found at `{conf_path}`.")
        filtered_lines = []
        with open(conf_path, mode="r", encoding="utf-8") as file:
            origin_lines = file.readlines()

        for line in origin_lines:
            line = line.strip()
            if line.startswith("#"):
                # append annotations directly
                filtered_lines.append(line + "\n")
            else:
                try:
                    key, value = line.split("=")
                    key = key.strip()
                    value = value.strip()
                    if key not in config:
                        append_line = f"{key} = {value}\n"
                        filtered_lines.append(append_line)

                except ValueError:
                    # cannot split to key, value, so just append
                    filtered_lines.append(line + "\n")

        with open(conf_path, mode="w", encoding="utf-8") as file:
            for line in filtered_lines:
                file.write(line)
