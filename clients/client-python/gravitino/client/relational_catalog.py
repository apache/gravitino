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
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table import Table
from gravitino.api.rel.table_catalog import TableCatalog
from gravitino.client.base_schema_catalog import BaseSchemaCatalog
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.requests.table_create_request import TableCreateRequest
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.table_response import TableResponse
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.exceptions.handlers.table_error_handler import TABLE_ERROR_HANDLER
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient


class RelationalCatalog(BaseSchemaCatalog, TableCatalog):
    """Relational catalog is a catalog implementation

    The `RelationalCatalog` supports relational database like metadata operations,
    for example, schemas and tables list, creation, update and deletion. A Relational
    catalog is under the metalake.
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
        """Return this relational catalog as a :class:`TableCatalog`.

        This method returns ``self`` to provide access to table-related
        operations defined by the :class:`TableCatalog` interface.

        Returns:
            TableCatalog: The current catalog instance as a ``TableCatalog``.
        """
        return self

    def _check_table_name_identifier(self, identifier: NameIdentifier) -> None:
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
        self._check_table_namespace(identifier.namespace())

    def _check_table_namespace(self, namespace: Namespace) -> None:
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

    def _get_table_full_namespace(self, table_namespace: Namespace) -> Namespace:
        """Get the full namespace of the table with the given table's short namespace (schema name).

        Args:
            table_namespace (Namespace): The table's short namespace, which is the schema name.

        Returns:
            Namespace: full namespace of the table, which is "metalake.catalog.schema" format.
        """
        return Namespace.of(
            self._catalog_namespace.level(0),
            self._name,
            table_namespace.level(0),
        )

    def _format_table_request_path(self, ns: Namespace) -> str:
        schema_ns = Namespace.of(ns.level(0), ns.level(1))
        return (
            f"{BaseSchemaCatalog.format_schema_request_path(schema_ns)}"
            f"/{encode_string(ns.level(2))}"
            "/tables"
        )

    def create_table(
        self,
        identifier: NameIdentifier,
        columns: list[Column],
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
        partitioning: Optional[list[Transform]] = None,
        distribution: Optional[Distribution] = None,
        sort_orders: Optional[list[SortOrder]] = None,
        indexes: Optional[list[Index]] = None,
    ) -> Table:
        self._check_table_name_identifier(identifier)
        req = TableCreateRequest(
            _name=identifier.name(),
            _columns=DTOConverters.to_dtos(columns),
            _comment=comment,
            _properties=properties,
            _sort_orders=DTOConverters.to_dtos(sort_orders),
            _distribution=(
                DTOConverters.to_dto(distribution)
                if distribution is not None
                else DistributionDTO.NONE
            ),
            _partitioning=DTOConverters.to_dtos(partitioning),
            _indexes=DTOConverters.to_dtos(indexes),
        )
        req.validate()
        full_namespace = self._get_table_full_namespace(identifier.namespace())
        resp = self.rest_client.post(
            self._format_table_request_path(full_namespace),
            json=req,
            error_handler=TABLE_ERROR_HANDLER,
        )
        table_resp = TableResponse.from_json(resp.body, infer_missing=True)
        table_resp.validate()
        return RelationalTable(full_namespace, table_resp.table(), self.rest_client)

    def list_tables(self, namespace: Namespace) -> list[NameIdentifier]:
        self._check_table_namespace(namespace)
        full_namespace = self._get_table_full_namespace(namespace)
        resp = self.rest_client.get(
            self._format_table_request_path(full_namespace),
            error_handler=TABLE_ERROR_HANDLER,
        )
        entity_list_resp = EntityListResponse.from_json(resp.body, infer_missing=True)
        entity_list_resp.validate()
        return [
            NameIdentifier.of(ident.namespace().level(2), ident.name())
            for ident in entity_list_resp.identifiers()
        ]

    def load_table(self, identifier: NameIdentifier) -> Table:
        self._check_table_name_identifier(identifier)
        full_namespace = self._get_table_full_namespace(identifier.namespace())
        resp = self.rest_client.get(
            f"{self._format_table_request_path(full_namespace)}"
            f"/{encode_string(identifier.name())}",
            error_handler=TABLE_ERROR_HANDLER,
        )
        table_resp = TableResponse.from_json(resp.body, infer_missing=True)
        table_resp.validate()
        return RelationalTable(full_namespace, table_resp.table(), self.rest_client)

    # TODO: We shall implement the following methods after integration tests for relational table
    def drop_table(self, identifier: NameIdentifier) -> bool:
        raise NotImplementedError("Drop table is not implemented yet.")

    def alter_table(self, identifier: NameIdentifier, *changes) -> Table:
        raise NotImplementedError("Alter table is not implemented yet.")

    def purge_table(self, identifier: NameIdentifier) -> bool:
        raise NotImplementedError("Purge table is not implemented yet.")
