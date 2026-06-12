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

from abc import ABC, abstractmethod


class CatalogOperation(ABC):
    """
    Abstract base class for Gravitino catalog operations.
    """

    @abstractmethod
    async def get_list_of_catalogs(self) -> str:
        """
        Retrieve the list of catalogs.

        Returns:
            str: JSON-formatted string containing catalog information.
        """
        pass

    @abstractmethod
    # pylint: disable=too-many-positional-arguments
    async def create_catalog(
        self,
        name: str,
        catalog_type: str,
        provider: str,
        comment: str,
        properties: dict,
    ) -> str:
        pass

    @abstractmethod
    async def alter_catalog(self, catalog_name: str, updates: list) -> str:
        pass

    @abstractmethod
    async def drop_catalog(self, catalog_name: str) -> str:
        pass
