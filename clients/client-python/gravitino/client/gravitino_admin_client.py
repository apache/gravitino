"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import logging
from typing import List, Dict

from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.requests.metalake_create_request import MetalakeCreateRequest
from gravitino.dto.requests.metalake_updates_request import MetalakeUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.metalake_list_response import MetalakeListResponse
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.meta_change import MetalakeChange
from gravitino.name_identifier import NameIdentifier

logger = logging.getLogger(__name__)


class GravitinoAdminClient(GravitinoClientBase):
    """
    Gravitino Client for the administrator to interact with the Gravitino API, allowing the client to list, load, create, and alter Metalakes.
    Normal users should use {@link GravitinoClient} to connect with the Gravitino server.
    """

    def __init__(self, uri):  # TODO: AuthDataProvider authDataProvider
        super().__init__(uri)

    def list_metalakes(self) -> List[GravitinoMetalake]:
        """
        Retrieves a list of Metalakes from the Gravitino API.
        Return:
            An array of GravitinoMetalake objects representing the Metalakes.
        """
        resp = self.rest_client.get(self.API_METALAKES_LIST_PATH)
        metalake_list_resp = MetalakeListResponse.from_json(resp.body, infer_missing=True)
        metalake_list_resp.validate()

        return [GravitinoMetalake.build(o, self.rest_client) for o in metalake_list_resp.metalakes]

    def create_metalake(self, ident: NameIdentifier, comment: str, properties: Dict[str, str]) -> GravitinoMetalake:
        """
        Creates a new Metalake using the Gravitino API.
        Args:
            ident: The identifier of the new Metalake.
            comment: The comment for the new Metalake.
            properties: The properties of the new Metalake.
        Return:
            A GravitinoMetalake instance representing the newly created Metalake.
        TODO: @throws MetalakeAlreadyExistsException If a Metalake with the specified identifier already exists.
        """
        NameIdentifier.check_metalake(ident)

        req = MetalakeCreateRequest(ident.name, comment, properties)
        req.validate()

        resp = self.rest_client.post(self.API_METALAKES_LIST_PATH, req)
        metalake_response = MetalakeResponse.from_json(resp.body, infer_missing=True)
        metalake_response.validate()

        return GravitinoMetalake.build(metalake_response.metalake, self.rest_client)

    def alter_metalake(self, ident: NameIdentifier, *changes: MetalakeChange) -> GravitinoMetalake:
        """
        Alters a specific Metalake using the Gravitino API.
        Args:
            ident: The identifier of the Metalake to be altered.
            changes: The changes to be applied to the Metalake.
        Return:
             A GravitinoMetalake instance representing the updated Metalake.
        TODO: @throws NoSuchMetalakeException If the specified Metalake does not exist.
        TODO: @throws IllegalArgumentException If the provided changes are invalid or not applicable.
        """
        NameIdentifier.check_metalake(ident)

        reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
        updates_request = MetalakeUpdatesRequest(reqs)
        updates_request.validate()

        resp = self.rest_client.put(self.API_METALAKES_IDENTIFIER_PATH + ident.name,
                                    updates_request)  # , MetalakeResponse, {}, ErrorHandlers.metalake_error_handler())
        metalake_response = MetalakeResponse.from_json(resp.body)
        metalake_response.validate()

        return GravitinoMetalake.build(metalake_response.metalake, self.rest_client)

    def drop_metalake(self, ident: NameIdentifier) -> bool:
        """
        Drops a specific Metalake using the Gravitino API.
        Args:
            ident: The identifier of the Metalake to be dropped.
        Return:
            True if the Metalake was successfully dropped, false otherwise.
        """
        NameIdentifier.check_metalake(ident)

        try:
            resp = self.rest_client.delete(self.API_METALAKES_IDENTIFIER_PATH + ident.name)
            dropResponse = DropResponse.from_json(resp.body)

            return dropResponse.dropped()

        except Exception as e:
            logger.warning(f"Failed to drop metadata ", e)
            return False
