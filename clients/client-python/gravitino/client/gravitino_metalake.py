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

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.requests.catalog_create_request import CatalogCreateRequest
from gravitino.dto.requests.catalog_updates_request import CatalogUpdatesRequest
from gravitino.dto.responses.catalog_list_response import CatalogListResponse
from gravitino.dto.responses.catalog_response import CatalogResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.exceptions.handlers.catalog_error_handler import CATALOG_ERROR_HANDLER
from gravitino.utils import HTTPClient


logger = logging.getLogger(__name__)


class GravitinoMetalake(MetalakeDTO):
    """
    Gravitino Metalake is the top-level metadata repository for users. It contains a list of catalogs
    as sub-level metadata collections. With GravitinoMetalake, users can list, create, load,
    alter and drop a catalog with specified identifier.
    """

    rest_client: HTTPClient

    API_METALAKES_CATALOGS_PATH = "api/metalakes/{}/catalogs/{}"

    def __init__(self, metalake: MetalakeDTO = None, client: HTTPClient = None):
        super().__init__(
            _name=metalake.name(),
            _comment=metalake.comment(),
            _properties=metalake.properties(),
            _audit=metalake.audit_info(),
        )
        self.rest_client = client

    def list_catalogs(self) -> List[str]:
        """List all the catalogs under this metalake.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.

        Returns:
            A list of the catalog names under this metalake.
        """
        url = f"api/metalakes/{self.name()}/catalogs"
        response = self.rest_client.get(url, error_handler=CATALOG_ERROR_HANDLER)
        entity_list = EntityListResponse.from_json(response.body, infer_missing=True)
        entity_list.validate()
        return [identifier.name() for identifier in entity_list.identifiers()]

    def list_catalogs_info(self) -> List[Catalog]:
        """List all the catalogs with their information under this metalake.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.

        Returns:
            A list of Catalog under the specified namespace.
        """
        params = {"details": "true"}
        url = f"api/metalakes/{self.name()}/catalogs"
        response = self.rest_client.get(
            url, params=params, error_handler=CATALOG_ERROR_HANDLER
        )
        catalog_list = CatalogListResponse.from_json(response.body, infer_missing=True)

        return [
            DTOConverters.to_catalog(self.name(), catalog, self.rest_client)
            for catalog in catalog_list.catalogs()
        ]

    def load_catalog(self, name: str) -> Catalog:
        """Load the catalog with specified name.

        Args:
            name: The name of the catalog to load.

        Raises:
            NoSuchCatalogException if the catalog with specified name does not exist.

        Returns:
            The Catalog with specified name.
        """
        url = self.API_METALAKES_CATALOGS_PATH.format(self.name(), name)
        response = self.rest_client.get(url, error_handler=CATALOG_ERROR_HANDLER)
        catalog_resp = CatalogResponse.from_json(response.body, infer_missing=True)

        return DTOConverters.to_catalog(
            self.name(), catalog_resp.catalog(), self.rest_client
        )

    def create_catalog(
        self,
        name: str,
        catalog_type: Catalog.Type,
        provider: str,
        comment: str,
        properties: Dict[str, str],
    ) -> Catalog:
        """Create a new catalog with specified name, catalog type, comment and properties.

        Args:
            name: The name of the catalog.
            catalog_type: The type of the catalog.
            provider: The provider of the catalog.
            comment: The comment of the catalog.
            properties: The properties of the catalog.

        Raises:
            NoSuchMetalakeException if the metalake does not exist.
            CatalogAlreadyExistsException if the catalog with specified name already exists.

        Returns:
            The created Catalog.
        """

        catalog_create_request = CatalogCreateRequest(
            name=name,
            catalog_type=catalog_type,
            provider=provider,
            comment=comment,
            properties=properties,
        )
        catalog_create_request.validate()

        url = f"api/metalakes/{self.name()}/catalogs"
        response = self.rest_client.post(
            url, json=catalog_create_request, error_handler=CATALOG_ERROR_HANDLER
        )
        catalog_resp = CatalogResponse.from_json(response.body, infer_missing=True)

        return DTOConverters.to_catalog(
            self.name(), catalog_resp.catalog(), self.rest_client
        )

    def alter_catalog(self, name: str, *changes: CatalogChange) -> Catalog:
        """Alter the catalog with specified name by applying the changes.

        Args:
            name: the name of the catalog.
            changes: the changes to apply to the catalog.

        Raises:
            NoSuchCatalogException if the catalog with specified name does not exist.
            IllegalArgumentException if the changes are invalid.

        Returns:
            the altered Catalog.
        """

        reqs = [DTOConverters.to_catalog_update_request(change) for change in changes]
        updates_request = CatalogUpdatesRequest(reqs)
        updates_request.validate()

        url = self.API_METALAKES_CATALOGS_PATH.format(self.name(), name)
        response = self.rest_client.put(
            url, json=updates_request, error_handler=CATALOG_ERROR_HANDLER
        )
        catalog_response = CatalogResponse.from_json(response.body, infer_missing=True)
        catalog_response.validate()

        return DTOConverters.to_catalog(
            self.name(), catalog_response.catalog(), self.rest_client
        )

    def drop_catalog(self, name: str) -> bool:
        """Drop the catalog with specified name.

        Args:
            name the name of the catalog.

        Returns:
            true if the catalog is dropped successfully, false otherwise.
        """
        try:
            url = self.API_METALAKES_CATALOGS_PATH.format(self.name(), name)
            response = self.rest_client.delete(url, error_handler=CATALOG_ERROR_HANDLER)

            drop_response = DropResponse.from_json(response.body, infer_missing=True)
            drop_response.validate()

            return drop_response.dropped()
        except Exception:
            logger.warning("Failed to drop catalog %s", name)
            return False
