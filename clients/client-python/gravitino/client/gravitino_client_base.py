"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging
import subprocess
from datetime import datetime
import pkg_resources

from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.dto.version_dto import VersionDTO
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.dto.responses.version_response import VersionResponse
from gravitino.name_identifier import NameIdentifier
from gravitino.utils import HTTPClient
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException

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

    def __init__(self, uri: str, check_version: bool = True):
        self._rest_client = HTTPClient(uri)
        if check_version:
            self.check_version()

    def load_metalake(self, ident: NameIdentifier) -> GravitinoMetalake:
        """Loads a specific Metalake from the Gravitino API.

        Args:
            ident The identifier of the Metalake to be loaded.

        Returns:
            A GravitinoMetalake instance representing the loaded Metalake.

        Raises:
            NoSuchMetalakeException If the specified Metalake does not exist.
        """

        NameIdentifier.check_metalake(ident)

        response = self._rest_client.get(
            GravitinoClientBase.API_METALAKES_IDENTIFIER_PATH + ident.name()
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
        version = pkg_resources.require("gravitino")[0].version
        git_commit = (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode("ascii")
            .strip()
        )
        compile_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        return GravitinoVersion(VersionDTO(version, compile_date, git_commit))

    def get_server_version(self) -> GravitinoVersion:
        """Retrieves the version of the Gravitino API.

        Returns:
            A GravitinoVersion instance representing the version of the Gravitino API.
        """
        resp = self._rest_client.get("api/version")
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
