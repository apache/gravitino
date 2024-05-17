"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging

from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.name_identifier import NameIdentifier
from gravitino.utils import HTTPClient

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

    def __init__(self, uri: str):
        self._rest_client = HTTPClient(uri)

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

    def get_version(self) -> GravitinoVersion:
        """Retrieves the version of the Gravitino API.

        Returns:
            A GravitinoVersion instance representing the version of the Gravitino API.
        """
        resp = self._rest_client.get("api/version")
        resp.validate()

        return GravitinoVersion(resp.get_version())

    def close(self):
        """Closes the GravitinoClient and releases any underlying resources."""
        if self._rest_client is not None:
            try:
                self._rest_client.close()
            except Exception as e:
                logger.warning("Failed to close the HTTP REST client: %s", e)
