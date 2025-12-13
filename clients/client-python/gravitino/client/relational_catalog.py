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
from gravitino.dto.responses.table_response import TableResponse
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
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
        comment: str,
        properties: dict[str, str],
        partitioning: Optional[list[Transform]] = None,
        distribution: Optional[Distribution] = None,
        sort_orders: Optional[list[SortOrder]] = None,
        indexes: Optional[list[Index]] = None,
    ) -> Table:
        RelationalCatalog.check_table_name_identifier(identifier)
        req = TableCreateRequest(
            _name=identifier.name(),
            _columns=DTOConverters.to_dtos(columns),
            _comment=comment,
            _properties=properties,
            _sort_orders=(
                DTOConverters.to_dtos(sort_orders) if sort_orders is not None else []
            ),
            _distribution=(
                DTOConverters.to_dto(distribution)
                if distribution is not None
                else DistributionDTO.NONE
            ),
            _partitioning=(
                DTOConverters.to_dtos(partitioning) if partitioning is not None else []
            ),
            _indexes=DTOConverters.to_dtos(indexes) if indexes is not None else [],
        )
        req.validate()
        full_namespace = self._get_table_full_namespace(identifier.namespace())
        rest_client_resp = self.rest_client.post(
            self._format_table_request_path(full_namespace),
            json=req,
        )
        resp = TableResponse.from_json(rest_client_resp.body, infer_missing=True)
        resp.validate()
        return RelationalTable(full_namespace, resp.table(), self.rest_client)
