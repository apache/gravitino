"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import List, Dict

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class NoSuchMetalakeException(Exception):
    pass


class NoSuchCatalogException(Exception):
    pass


class CatalogAlreadyExistsException(Exception):
    pass


class GravitinoClient(GravitinoClientBase):
    """Gravitino Client for an user to interact with the Gravitino API, allowing the client to list,
    load, create, and alter Catalog.

    It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the API.
    """

    _metalake: GravitinoMetalake

    def __init__(self, uri: str, metalake_name: str):
        """Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.

        Args:
            uri: The base URI for the Gravitino API.
            metalake_name: The specified metalake name.
            TODO: authDataProvider: The provider of the data which is used for authentication.

        Raises:
            NoSuchMetalakeException if the metalake with specified name does not exist.
        """
        super().__init__(uri)
        self._metalake = super().load_metalake(NameIdentifier.of(metalake_name))

    def get_metalake(self) -> GravitinoMetalake:
        """Get the current metalake object

        Raises:
            NoSuchMetalakeException if the metalake with specified name does not exist.

        Returns:
            the GravitinoMetalake object
        """
        return self._metalake

    def list_catalogs(self, namespace: Namespace) -> List[NameIdentifier]:
        return self.get_metalake().list_catalogs(namespace)

    def list_catalogs_info(self, namespace: Namespace) -> List[Catalog]:
        return self.get_metalake().list_catalogs_info(namespace)

    def load_catalog(self, ident: NameIdentifier) -> Catalog:
        return self.get_metalake().load_catalog(ident)

    def create_catalog(
        self,
        ident: NameIdentifier,
        type: Catalog.Type,
        provider: str,
        comment: str,
        properties: Dict[str, str],
    ) -> Catalog:
        return self.get_metalake().create_catalog(
            ident, type, provider, comment, properties
        )

    def alter_catalog(self, ident: NameIdentifier, *changes: CatalogChange):
        return self.get_metalake().alter_catalog(ident, *changes)

    def drop_catalog(self, ident: NameIdentifier):
        return self.get_metalake().drop_catalog(ident)
