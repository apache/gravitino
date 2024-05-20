"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
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
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient


logger = logging.getLogger(__name__)


class NoSuchMetalakeException(Exception):
    pass


class NoSuchCatalogException(Exception):
    pass


class CatalogAlreadyExistsException(Exception):
    pass


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

    def list_catalogs(self, namespace: Namespace) -> List[NameIdentifier]:
        """List all the catalogs under this metalake with specified namespace.

        Args:
            namespace The namespace to list the catalogs under it.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.

        Returns:
            A list of {@link NameIdentifier} of the catalogs under the specified namespace.
        """
        Namespace.check_catalog(namespace)
        url = f"api/metalakes/{namespace.level(0)}/catalogs"
        response = self.rest_client.get(url)
        entity_list = EntityListResponse.from_json(response.body, infer_missing=True)
        entity_list.validate()
        return entity_list.identifiers()

    def list_catalogs_info(self, namespace: Namespace) -> List[Catalog]:
        """List all the catalogs with their information under this metalake with specified namespace.

        Args:
            namespace The namespace to list the catalogs under it.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.

        Returns:
            A list of Catalog under the specified namespace.
        """
        Namespace.check_catalog(namespace)
        params = {"details": "true"}
        url = f"api/metalakes/{namespace.level(0)}/catalogs"
        response = self.rest_client.get(url, params=params)
        catalog_list = CatalogListResponse.from_json(response.body, infer_missing=True)

        return [
            DTOConverters.to_catalog(catalog, self.rest_client)
            for catalog in catalog_list.catalogs()
        ]

    def load_catalog(self, ident: NameIdentifier) -> Catalog:
        """Load the catalog with specified identifier.

        Args:
            ident: The identifier of the catalog to load.

        Raises:
            NoSuchCatalogException if the catalog with specified identifier does not exist.

        Returns:
            The Catalog with specified identifier.
        """
        NameIdentifier.check_catalog(ident)
        url = self.API_METALAKES_CATALOGS_PATH.format(
            ident.namespace().level(0), ident.name()
        )
        response = self.rest_client.get(url)
        catalog_resp = CatalogResponse.from_json(response.body, infer_missing=True)

        return DTOConverters.to_catalog(catalog_resp.catalog(), self.rest_client)

    def create_catalog(
        self,
        ident: NameIdentifier,
        type: Catalog.Type,
        provider: str,
        comment: str,
        properties: Dict[str, str],
    ) -> Catalog:
        """Create a new catalog with specified identifier, type, comment and properties.

        Args:
            ident: The identifier of the catalog.
            type: The type of the catalog.
            provider: The provider of the catalog.
            comment: The comment of the catalog.
            properties: The properties of the catalog.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.
            CatalogAlreadyExistsException if the catalog with specified identifier already exists.

        Returns:
            The created Catalog.
        """
        NameIdentifier.check_catalog(ident)

        catalog_create_request = CatalogCreateRequest(
            name=ident.name(),
            type=type,
            provider=provider,
            comment=comment,
            properties=properties,
        )
        catalog_create_request.validate()

        url = f"api/metalakes/{ident.namespace().level(0)}/catalogs"
        response = self.rest_client.post(url, json=catalog_create_request)
        catalog_resp = CatalogResponse.from_json(response.body, infer_missing=True)

        return DTOConverters.to_catalog(catalog_resp.catalog(), self.rest_client)

    def alter_catalog(self, ident: NameIdentifier, *changes: CatalogChange) -> Catalog:
        """Alter the catalog with specified identifier by applying the changes.

        Args:
            ident: the identifier of the catalog.
            changes: the changes to apply to the catalog.

        Raises:
            NoSuchCatalogException if the catalog with specified identifier does not exist.
            IllegalArgumentException if the changes are invalid.

        Returns:
            the altered Catalog.
        """
        NameIdentifier.check_catalog(ident)

        reqs = [DTOConverters.to_catalog_update_request(change) for change in changes]
        updates_request = CatalogUpdatesRequest(reqs)
        updates_request.validate()

        url = self.API_METALAKES_CATALOGS_PATH.format(
            ident.namespace().level(0), ident.name()
        )
        response = self.rest_client.put(url, json=updates_request)
        catalog_response = CatalogResponse.from_json(response.body, infer_missing=True)
        catalog_response.validate()

        return DTOConverters.to_catalog(catalog_response.catalog(), self.rest_client)

    def drop_catalog(self, ident: NameIdentifier) -> bool:
        """Drop the catalog with specified identifier.

        Args:
            ident the identifier of the catalog.

        Returns:
            true if the catalog is dropped successfully, false otherwise.
        """
        try:
            url = self.API_METALAKES_CATALOGS_PATH.format(
                ident.namespace().level(0), ident.name()
            )
            response = self.rest_client.delete(url)

            drop_response = DropResponse.from_json(response.body, infer_missing=True)
            drop_response.validate()

            return drop_response.dropped()
        except Exception:
            logger.warning("Failed to drop catalog %s", ident)
            return False
