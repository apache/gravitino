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

import asyncio
import logging
import os
import time

from docker.errors import DockerException
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.exceptions.base import InternalError

from tests.integration.containers.base_container import BaseContainer

logger = logging.getLogger(__name__)


async def check_hdfs_status(hdfs_container):
    retry_limit = 15
    for _ in range(retry_limit):
        try:
            command_and_args = ["bash", "/tmp/check-status.sh"]
            exec_result = hdfs_container.exec_run(command_and_args)
            if exec_result.exit_code != 0:
                message = (
                    f"Command {command_and_args} exited with {exec_result.exit_code}"
                )
                logger.warning(message)
                logger.warning("output: %s", exec_result.output)
                output_status_command = ["hdfs", "dfsadmin", "-report"]
                exec_result = hdfs_container.exec_run(output_status_command)
                logger.info("HDFS report, output: %s", exec_result.output)
            else:
                logger.info("HDFS startup successfully!")
                return True
        except DockerException as e:
            logger.error(
                "Exception occurred while checking HDFS container status: %s", e
            )
        time.sleep(10)
    return False


async def check_hdfs_container_status(hdfs_container):
    timeout_sec = 150
    try:
        result = await asyncio.wait_for(
            check_hdfs_status(hdfs_container), timeout=timeout_sec
        )
        if not result:
            raise InternalError("HDFS container startup failed!")
    except asyncio.TimeoutError as e:
        raise GravitinoRuntimeException(
            "Timeout occurred while waiting for checking HDFS container status."
        ) from e


class HDFSContainer(BaseContainer):

    def __init__(self):
        container_name = "python-hdfs"
        image_name = os.environ.get("GRAVITINO_CI_HIVE_DOCKER_IMAGE")
        if image_name is None:
            raise GravitinoRuntimeException(
                "GRAVITINO_CI_HIVE_DOCKER_IMAGE env variable is not set."
            )
        environment = {"HADOOP_USER_NAME": "anonymous"}

        super().__init__(container_name, image_name, environment)

        asyncio.run(check_hdfs_container_status(self._container))
        self._fetch_ip()
