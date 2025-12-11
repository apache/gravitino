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

from typing import Optional

from gravitino.api.catalog import Catalog
from gravitino.api.rel.table_catalog import TableCatalog
from gravitino.client.base_schema_catalog import BaseSchemaCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient


class RelationalCatalog(BaseSchemaCatalog, TableCatalog):
    """Relational catalog is a catalog implementation that supports relational database.

    It's like metadata operations, for example, schemas and tables list, creation, update
    and deletion. A Relational catalog is under the metalake.
    """

    def __init__(
        self,
        catalog_namespace: Namespace,
        name: str,
        catalog_type: Catalog.Type,
        provider: str,
        audit: AuditDTO,
        rest_client: HTTPClient,
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
    ):
        super().__init__(
            catalog_namespace,
            name,
            catalog_type,
            provider,
            comment,
            properties,
            audit,
            rest_client,
        )

    def as_table_catalog(self) -> TableCatalog:
        return self

    @staticmethod
    def check_table_name_identifier(identifier: NameIdentifier) -> None:
        """Check whether the `NameIdentifier` of a table is valid.
        Args:
            identifier (NameIdentifier):
                The NameIdentifier to check, which should be "schema.table" format.

        Raises:
            IllegalNameIdentifierException: If the Namespace is not valid.
        """
        NameIdentifier.check(identifier is not None, "NameIdentifier must not be null")
        NameIdentifier.check(
            identifier.name() is not None and identifier.name() != "",
            "NameIdentifier name must not be empty",
        )
        RelationalCatalog.check_table_namespace(identifier.namespace())

    @staticmethod
    def check_table_namespace(namespace: Namespace) -> None:
        """Check whether the namespace of a table is valid, which should be "schema".

        Args:
            namespace (Namespace): The namespace to check.

        Raises:
            IllegalNamespaceException: If the Namespace is not valid.
        """
        Namespace.check(
            namespace is not None and namespace.length() == 1,
            f"Table namespace must be non-null and have 1 level, the input namespace is {namespace}",
        )
