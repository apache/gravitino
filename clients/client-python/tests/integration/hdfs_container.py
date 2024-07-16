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

import asyncio
import logging
import os
import time

import docker
from docker import types as tp
from docker.errors import NotFound

from gravitino.exceptions.base import GravitinoRuntimeException

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
        except Exception as e:
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
        assert result is True, "HDFS container startup failed!"
    except asyncio.TimeoutError as e:
        raise GravitinoRuntimeException(
            "Timeout occurred while waiting for checking HDFS container status."
        ) from e


class HDFSContainer:
    _docker_client = None
    _container = None
    _network = None
    _ip = ""
    _network_name = "python-net"
    _container_name = "python-hdfs"

    def __init__(self):
        self._docker_client = docker.from_env()
        self._create_networks()
        try:
            container = self._docker_client.containers.get(self._container_name)
            if container is not None:
                if container.status != "running":
                    container.restart()
                self._container = container
        except NotFound:
            logger.warning("Cannot find hdfs container in docker env, skip remove.")
        if self._container is None:
            image_name = os.environ.get("GRAVITINO_CI_HIVE_DOCKER_IMAGE")
            if image_name is None:
                raise GravitinoRuntimeException(
                    "GRAVITINO_CI_HIVE_DOCKER_IMAGE env variable is not set."
                )
            self._container = self._docker_client.containers.run(
                image=image_name,
                name=self._container_name,
                detach=True,
                environment={"HADOOP_USER_NAME": "anonymous"},
                network=self._network_name,
            )
        asyncio.run(check_hdfs_container_status(self._container))

        self._fetch_ip()

    def _create_networks(self):
        pool_config = tp.IPAMPool(subnet="10.20.31.16/28")
        ipam_config = tp.IPAMConfig(driver="default", pool_configs=[pool_config])
        networks = self._docker_client.networks.list()
        for network in networks:
            if network.name == self._network_name:
                self._network = network
                break
        if self._network is None:
            self._network = self._docker_client.networks.create(
                name=self._network_name, driver="bridge", ipam=ipam_config
            )

    def _fetch_ip(self):
        if self._container is None:
            raise GravitinoRuntimeException("The HDFS container has not init.")

        container_info = self._docker_client.api.inspect_container(self._container.id)
        self._ip = container_info["NetworkSettings"]["Networks"][self._network_name][
            "IPAddress"
        ]

    def get_ip(self):
        return self._ip

    def close(self):
        try:
            self._container.kill()
        except RuntimeError as e:
            logger.warning(
                "Exception occurred while killing container %s : %s",
                self._container_name,
                e,
            )
        try:
            self._container.remove()
        except RuntimeError as e:
            logger.warning(
                "Exception occurred while removing container %s : %s",
                self._container_name,
                e,
            )
        try:
            self._network.remove()
        except RuntimeError as e:
            logger.warning(
                "Exception occurred while removing network %s : %s",
                self._network_name,
                e,
            )
