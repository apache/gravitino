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


class SchemaOperation(ABC):
    """
    Abstract base class for Gravitino schema operations.
    """

    @abstractmethod
    async def get_list_of_schemas(self, catalog_name: str) -> str:
        """
        Retrieve the list of schemas under a specified catalog.

        Args:
            catalog_name: Name of the catalog

        Returns:
            str: JSON-formatted string containing catalog information.
        """
        pass

    @abstractmethod
    async def create_schema(
        self, catalog_name: str, name: str, comment: str, properties: dict
    ) -> str:
        pass

    @abstractmethod
    async def alter_schema(
        self, catalog_name: str, schema_name: str, updates: list
    ) -> str:
        pass

    @abstractmethod
    async def drop_schema(
        self, catalog_name: str, schema_name: str, cascade: bool
    ) -> str:
        pass
