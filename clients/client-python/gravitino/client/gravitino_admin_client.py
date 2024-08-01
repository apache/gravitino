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
from typing import List, Dict

from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.requests.metalake_create_request import MetalakeCreateRequest
from gravitino.dto.requests.metalake_updates_request import MetalakeUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.metalake_list_response import MetalakeListResponse
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.api.metalake_change import MetalakeChange
from gravitino.exceptions.handlers.metalake_error_handler import METALAKE_ERROR_HANDLER

logger = logging.getLogger(__name__)


class GravitinoAdminClient(GravitinoClientBase):
    """
    Gravitino Client for the administrator to interact with the Gravitino API.
    It allows the client to list, load, create, and alter Metalakes.
    Normal users should use {@link GravitinoClient} to connect with the Gravitino server.
    """

    def list_metalakes(self) -> List[GravitinoMetalake]:
        """Retrieves a list of Metalakes from the Gravitino API.

        Returns:
            An array of GravitinoMetalake objects representing the Metalakes.
        """
        resp = self._rest_client.get(
            self.API_METALAKES_LIST_PATH, error_handler=METALAKE_ERROR_HANDLER
        )
        metalake_list_resp = MetalakeListResponse.from_json(
            resp.body, infer_missing=True
        )
        metalake_list_resp.validate()

        return [
            GravitinoMetalake(o, self._rest_client)
            for o in metalake_list_resp.metalakes()
        ]

    def create_metalake(
        self, name: str, comment: str, properties: Dict[str, str]
    ) -> GravitinoMetalake:
        """Creates a new Metalake using the Gravitino API.

        Args:
            name: The name of the new Metalake.
            comment: The comment for the new Metalake.
            properties: The properties of the new Metalake.

        Returns:
            A GravitinoMetalake instance representing the newly created Metalake.
            TODO: @throws MetalakeAlreadyExistsException If a Metalake with the specified identifier already exists.
        """
        req = MetalakeCreateRequest(name, comment, properties)
        req.validate()

        resp = self._rest_client.post(
            self.API_METALAKES_LIST_PATH, req, error_handler=METALAKE_ERROR_HANDLER
        )
        metalake_response = MetalakeResponse.from_json(resp.body, infer_missing=True)
        metalake_response.validate()
        metalake = metalake_response.metalake()

        return GravitinoMetalake(metalake, self._rest_client)

    def alter_metalake(self, name: str, *changes: MetalakeChange) -> GravitinoMetalake:
        """Alters a specific Metalake using the Gravitino API.

        Args:
            name: The name of the Metalake to be altered.
            changes: The changes to be applied to the Metalake.

        Returns:
             A GravitinoMetalake instance representing the updated Metalake.
        TODO: @throws NoSuchMetalakeException If the specified Metalake does not exist.
        TODO: @throws IllegalArgumentException If the provided changes are invalid or not applicable.
        """

        reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
        updates_request = MetalakeUpdatesRequest(reqs)
        updates_request.validate()

        resp = self._rest_client.put(
            self.API_METALAKES_IDENTIFIER_PATH + name,
            updates_request,
            error_handler=METALAKE_ERROR_HANDLER,
        )
        metalake_response = MetalakeResponse.from_json(resp.body, infer_missing=True)
        metalake_response.validate()
        metalake = metalake_response.metalake()

        return GravitinoMetalake(metalake, self._rest_client)

    def drop_metalake(self, name: str) -> bool:
        """Drops a specific Metalake using the Gravitino API.

        Args:
            name: The name of the Metalake to be dropped.

        Returns:
            True if the Metalake was successfully dropped, false otherwise.
        """
        try:
            resp = self._rest_client.delete(
                self.API_METALAKES_IDENTIFIER_PATH + name,
                error_handler=METALAKE_ERROR_HANDLER,
            )
            drop_response = DropResponse.from_json(resp.body, infer_missing=True)

            return drop_response.dropped()
        except Exception:
            logger.warning("Failed to drop metalake %s", name)
            return False
