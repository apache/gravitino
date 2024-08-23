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
from typing import Dict, List, Optional

from gravitino import Catalog
from gravitino.catalog.base_schema_catalog import BaseSchemaCatalog
from gravitino.namespace import Namespace
from gravitino.name_identifier import NameIdentifier
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table_catalog import TableCatalog
from gravitino.api.table import Table
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.requests.table_create_request import TableCreateRequest
from gravitino.dto.responses.table_response import TableResponse
from gravitino.exceptions.handlers.table_error_handler import TABLE_ERROR_HANDLER
from gravitino.rest import encode_string
from gravitino.utils import HTTPClient


logger = logging.getLogger(__name__)


class RelationalCatalog(BaseSchemaCatalog, TableCatalog):

    def __init__(
        self,
        catalog_namespace: Namespace,
        name: str = None,
        catalog_type: Catalog.Type = Catalog.Type.UNSUPPORTED,
        provider: str = None,
        comment: str = None,
        properties: Dict[str, str] = None,
        audit: AuditDTO = None,
        rest_client: HTTPClient = None,
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
        self._catalog_namespace = catalog_namespace
        self.rest_client = rest_client

    @staticmethod
    def format_table_request_path(namespace: Namespace) -> str:
        schema_ns = Namespace.of(namespace.level(0), namespace.level(1))
        return f"{BaseSchemaCatalog.format_schema_request_path(schema_ns)}/{encode_string(namespace.level(2))}/tables"

    @staticmethod
    def check_table_namespace(namespace: Namespace):
        """
        Check whether the namespace of a table is valid, which should be "schema".

        :param namespace: The namespace to check.
        """
        Namespace.check(
            namespace is not None and namespace.length() == 3,
            f"Table namespace must be non-null and have 3 level, the input namespace is {namespace}",
        )

    @staticmethod
    def check_table_name_identifier(identifier: NameIdentifier):
        """
        Check whether the NameIdentifier of a table is valid.

        :param identifier: The NameIdentifier to checkm, which should be "schema.table" format.
        """
        NameIdentifier.check(identifier is not None, "Identifier name cannot be None")
        NameIdentifier.check(
            identifier.name() is not None, "Identifier name cannot be None"
        )
        RelationalCatalog.check_table_namespace(identifier.namespace())

    def as_table_catalog(self) -> "TableCatalog":
        return self

    def _get_table_full_namespace(self, table_namespace: Namespace) -> Namespace:
        """
        Get the full namespace of the table with the given table's short namespace (schema name).

        Args:
            table_namespace (Namespace): The namespace of the table.

        Returns:
            Namespace: The full namespace for the table, which is "metalake.catalog.schema" format.
        """
        return Namespace.of(
            self._catalog_namespace.level(0), self.name(), table_namespace.level(0)
        )

    def create_table(
        self,
        ident: NameIdentifier,
        columns: List[ColumnDTO],
        comment: str,
        properties: Dict[str, str],
        partitions: Optional[List[Transform]] = [],
        distribution: Optional[Distribution] = None,
        sort_orders: Optional[List[SortOrder]] = [],
        indexes: Optional[List[Index]] = [],
    ) -> Table:
        """
        Create a new table with specified identifier, columns, comment and properties.

        Args:
            ident (NameIdentifier): The identifier of the table.
            columns (List[Column]): The columns of the table.
            comment (str): The comment of the table.
            properties (Dict[str, str]): The properties of the table.
            partitions (List[Transform]): The partitions of the table.
            distribution (Distribution): The distribution of the table.
            sort_orders (List[SortOrder]): The sort orders of the table.
            indexes (List[Index]): The indexes of the table.

        Returns:
            Table: The created table.

        Raises:
            TableAlreadyExistsException: If the table already exists.
            NoSuchSchemaException: If the schema does not exist.
            UnsupportedOperationException: If the operation is not supported.
        """
        self.check_table_name_identifier(ident)

        req = TableCreateRequest(
            name=ident.name(),
            comment=comment,
            properties=properties,
            columns=columns,
        )

        full_namespace = self._get_table_full_namespace(ident.namespace())
        resp = self.rest_client.post(
            self.format_table_request_path(full_namespace),
            req,
            TABLE_ERROR_HANDLER,
        )
        table_resp = TableResponse.from_json(resp.body, infer_missing=True)
        table_resp.validate()

        return table_resp.table()

    def list_tables(self, namespace: Namespace) -> List[NameIdentifier]:
        pass

    def load_table(self, ident: NameIdentifier) -> Table:
        pass

    def alter_table(
        self,
        ident: NameIdentifier,
        columns: List[ColumnDTO] = None,
        comment: str = None,
        properties: Dict[str, str] = None,
    ) -> Table:
        pass

    def drop_table(self, ident: NameIdentifier) -> bool:
        pass
