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

from mcp_server.connector.catalog_operation import CatalogOperation
from mcp_server.connector.schema_operation import SchemaOperation
from mcp_server.connector.table_operation import TableOperation
from mcp_server.connector.tag_operation import TagOperation


class GravitinoOperation(ABC):
    """
    Abstract base class for Gravitino operations with multiple operation facets.
    """

    @abstractmethod
    def as_table_operation(self) -> TableOperation:
        """
        Access the table operation interface of this Gravitino operation.

        Returns:
            TableOperation: Interface for performing table-level operations
        """
        pass

    @abstractmethod
    def as_schema_operation(self) -> SchemaOperation:
        """
        Access the schema operation interface of this Gravitino operation.

        Returns:
            SchemaOperation: Interface for performing schema-level operations
        """
        pass

    @abstractmethod
    def as_catalog_operation(self) -> CatalogOperation:
        """
        Access the catalog operation interface of this Gravitino operation.

        Returns:
            CatalogOperation: Interface for performing catalog-level operations
        """
        pass

    @abstractmethod
    def as_tag_operation(self) -> TagOperation:
        """
        Access the tag operation interface of this Gravitino operation.

        Returns:
            TagOperation: Interface for performing tag-level operations
        """
        pass
