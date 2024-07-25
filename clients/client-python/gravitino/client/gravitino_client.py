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

from typing import List, Dict

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake


class GravitinoClient(GravitinoClientBase):
    """Gravitino Client for a user to interact with the Gravitino API, allowing the client to list,
    load, create, and alter Catalog.

    It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the API.
    """

    _metalake: GravitinoMetalake

    def __init__(
        self,
        uri: str,
        metalake_name: str,
        check_version: bool = True,
        auth_data_provider: AuthDataProvider = None,
    ):
        """Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.

        Args:
            uri: The base URI for the Gravitino API.
            metalake_name: The specified metalake name.
            auth_data_provider: The provider of the data which is used for authentication.

        Raises:
            NoSuchMetalakeException if the metalake with specified name does not exist.
        """
        super().__init__(uri, check_version, auth_data_provider)
        self.check_metalake_name(metalake_name)
        self._metalake = super().load_metalake(metalake_name)

    def get_metalake(self) -> GravitinoMetalake:
        """Get the current metalake object

        Raises:
            NoSuchMetalakeException if the metalake with specified name does not exist.

        Returns:
            the GravitinoMetalake object
        """
        return self._metalake

    def list_catalogs(self) -> List[str]:
        return self.get_metalake().list_catalogs()

    def list_catalogs_info(self) -> List[Catalog]:
        return self.get_metalake().list_catalogs_info()

    def load_catalog(self, name: str) -> Catalog:
        return self.get_metalake().load_catalog(name)

    def create_catalog(
        self,
        name: str,
        catalog_type: Catalog.Type,
        provider: str,
        comment: str,
        properties: Dict[str, str],
    ) -> Catalog:
        return self.get_metalake().create_catalog(
            name, catalog_type, provider, comment, properties
        )

    def alter_catalog(self, name: str, *changes: CatalogChange):
        return self.get_metalake().alter_catalog(name, *changes)

    def drop_catalog(self, name: str):
        return self.get_metalake().drop_catalog(name)
