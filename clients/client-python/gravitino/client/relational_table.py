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

from typing import Optional, cast

from gravitino.api.audit import Audit
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.sorts.sort_order import SortOrder
from gravitino.api.rel.expressions.transforms.transform import Transform
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table import Table
from gravitino.client.generic_column import GenericColumn
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient


class RelationalTable(Table):  # pylint: disable=too-many-instance-attributes
    """Represents a relational table."""

    def __init__(
        self,
        name: str,
        columns: list[Column],
        partitioning: list[Transform],
        sort_order: list[SortOrder],
        distribution: Distribution,
        index: list[Index],
        comment: Optional[str],
        properties: dict[str, str],
        audit_info: Audit,
        namespace: Namespace,
        rest_client: HTTPClient,
    ):
        self._name = name
        self._columns = columns
        self._partitioning = partitioning
        self._sort_order = sort_order
        self._distribution = distribution
        self._index = index
        self._comment = comment
        self._properties = properties
        self._audit_info = audit_info
        self._namespace = namespace
        self._rest_client = rest_client

    def name(self) -> str:
        return self._name

    def columns(self) -> list[Column]:
        metalake, catalog, schema = self._namespace.levels()
        return [
            cast(
                Column,
                GenericColumn(
                    c, self._rest_client, metalake, catalog, schema, self._name
                ),
            )
            for c in self._columns
        ]

    def partitioning(self) -> list[Transform]:
        return self._partitioning

    def sort_order(self) -> list[SortOrder]:
        return self._sort_order

    def distribution(self) -> Distribution:
        return self._distribution

    def index(self) -> list[Index]:
        return self._index

    def comment(self) -> Optional[str]:
        return self._comment

    def properties(self) -> dict[str, str]:
        return self._properties

    def audit_info(self) -> Audit:
        return self._audit_info
