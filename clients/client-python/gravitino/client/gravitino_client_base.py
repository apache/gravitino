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
import configparser
import os.path

from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.dto.version_dto import VersionDTO
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.dto.responses.version_response import VersionResponse
from gravitino.exceptions.handlers.metalake_error_handler import METALAKE_ERROR_HANDLER
from gravitino.exceptions.handlers.rest_error_handler import REST_ERROR_HANDLER
from gravitino.utils import HTTPClient
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.constants.version import VERSION_INI, Version
from gravitino.name_identifier import NameIdentifier

logger = logging.getLogger(__name__)


class GravitinoClientBase:
    """
    Base class for Gravitino Java client;
    It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the API.
    """

    _rest_client: HTTPClient
    """The REST client to communicate with the REST server"""

    API_METALAKES_LIST_PATH = "api/metalakes"
    """The REST API path for listing metalakes"""

    API_METALAKES_IDENTIFIER_PATH = f"{API_METALAKES_LIST_PATH}/"
    """The REST API path prefix for load a specific metalake"""

    def __init__(
        self,
        uri: str,
        check_version: bool = True,
        auth_data_provider: AuthDataProvider = None,
    ):
        self._rest_client = HTTPClient(uri, auth_data_provider=auth_data_provider)
        if check_version:
            self.check_version()

    def load_metalake(self, name: str) -> GravitinoMetalake:
        """Loads a specific Metalake from the Gravitino API.

        Args:
            name: The name of the Metalake to be loaded.

        Returns:
            A GravitinoMetalake instance representing the loaded Metalake.

        Raises:
            NoSuchMetalakeException If the specified Metalake does not exist.
        """

        self.check_metalake_name(name)
        response = self._rest_client.get(
            GravitinoClientBase.API_METALAKES_IDENTIFIER_PATH + name,
            error_handler=METALAKE_ERROR_HANDLER,
        )
        metalake_response = MetalakeResponse.from_json(
            response.body, infer_missing=True
        )
        metalake_response.validate()

        return GravitinoMetalake(metalake_response.metalake(), self._rest_client)

    def check_version(self):
        """Check the compatibility of the client with the target server.

        Raises:
            GravitinoRuntimeException If the client version is greater than the server version.
        """
        server_version = self.get_server_version()
        client_version = self.get_client_version()

        if client_version > server_version:
            raise GravitinoRuntimeException(
                "Gravitino does not support the case that "
                "the client-side version is higher than the server-side version."
                f"The client version is {client_version.version()}, and the server version {server_version.version()}"
            )

    def get_client_version(self) -> GravitinoVersion:
        """Retrieves the version of the Gravitino Python Client.

        Returns:
            A GravitinoVersion instance representing the version of the Gravitino Python Client.
        """
        config = configparser.ConfigParser()

        if not os.path.exists(VERSION_INI):
            raise GravitinoRuntimeException(
                f"Failed to get Gravitino version, version file '{VERSION_INI}' does not exist."
            )
        config.read(VERSION_INI)

        version = config["metadata"][Version.VERSION.value]
        compile_date = config["metadata"][Version.COMPILE_DATE.value]
        git_commit = config["metadata"][Version.GIT_COMMIT.value]

        return GravitinoVersion(VersionDTO(version, compile_date, git_commit))

    def get_server_version(self) -> GravitinoVersion:
        """Retrieves the version of the Gravitino API.

        Returns:
            A GravitinoVersion instance representing the version of the Gravitino API.
        """
        resp = self._rest_client.get("api/version", error_handler=REST_ERROR_HANDLER)
        version_response = VersionResponse.from_json(resp.body, infer_missing=True)
        version_response.validate()

        return GravitinoVersion(version_response.version())

    def close(self):
        """Closes the GravitinoClient and releases any underlying resources."""
        if self._rest_client is not None:
            try:
                self._rest_client.close()
            except Exception as e:
                logger.warning("Failed to close the HTTP REST client: %s", e)

    def check_metalake_name(self, metalake_name: str):
        identifier = NameIdentifier.parse(metalake_name)
        namespace = identifier.namespace()

        if not namespace:
            raise ValueError(
                f"Metalake namespace must be empty, the input namespace is {namespace}"
            )
