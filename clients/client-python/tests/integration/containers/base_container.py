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
from typing import Dict

import docker
from docker import types as tp
from docker.errors import NotFound

from gravitino.exceptions.base import GravitinoRuntimeException

logger = logging.getLogger(__name__)


class BaseContainer:
    _docker_client = None
    _container = None
    _network = None
    _ip = ""
    _network_name = "python-net"
    _container_name: str

    def __init__(
        self, container_name: str, image_name: str, enviroment: Dict = None, **kwarg
    ):
        self._container_name = container_name
        self._docker_client = docker.from_env()
        self._create_networks()

        try:
            container = self._docker_client.containers.get(self._container_name)
            if container is not None:
                if container.status != "running":
                    container.restart()
                self._container = container
        except NotFound:
            logger.warning(
                "Cannot find the container %s in docker env, skip remove.",
                self._container_name,
            )
        if self._container is None:
            self._container = self._docker_client.containers.run(
                image=image_name,
                name=self._container_name,
                detach=True,
                environment=enviroment,
                network=self._network_name,
                **kwarg,
            )

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
            raise GravitinoRuntimeException(
                f"The container {self._container_name} has not init."
            )

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
