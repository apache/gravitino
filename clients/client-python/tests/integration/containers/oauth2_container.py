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
import os
import time

from gravitino.exceptions.base import GravitinoRuntimeException

from tests.integration.containers.base_container import BaseContainer

TIEMOUT_SEC = 5
RETRY_LIMIT = 30


async def check_oauth2_container_status(oauth2_container: "OAuth2Container"):
    for _ in range(RETRY_LIMIT):
        if oauth2_container.health_check():
            return True
        time.sleep(TIEMOUT_SEC)
    return False


class OAuth2Container(BaseContainer):

    def __init__(self):
        container_name = "sample-auth-server"
        image_name = os.environ.get("GRAVITINO_OAUTH2_SAMPLE_SERVER")
        if image_name is None:
            raise GravitinoRuntimeException(
                "GRAVITINO_OAUTH2_SAMPLE_SERVER env variable is not set."
            )

        healthcheck = {
            "test": [
                "CMD-SHELL",
                "wget -qO - http://localhost:8177/oauth2/jwks || exit 1",
            ],
            "interval": TIEMOUT_SEC * 1000000000,
            "retries": RETRY_LIMIT,
        }

        super().__init__(container_name, image_name, healthcheck=healthcheck)
        asyncio.run(check_oauth2_container_status(self))
        self._fetch_ip()

    def health_check(self) -> bool:
        return (
            self._docker_client.api.inspect_container(self._container_name)["State"][
                "Health"
            ]["Status"]
            == "healthy"
        )
